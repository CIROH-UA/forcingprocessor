"""Weight generation kernel: catchment polygons into grid cells and coverage fractions.

    (divides GeoDataFrame, grid template) -> WeightTable

A WeightTable is a DataFrame indexed by divide_id with two list columns, ``cell_id`` (flat indices
into the full NWM grid, Fortran order) and ``coverage`` (the weight of that cell for the catchment).
Any scheme expressible as a per catchment linear combination of grid cells, normalized by the sum of
its coefficients, fits this table. This module is the extension point for an alternative weight
generation scheme; the kernel that consumes the table is in averaging.py.
"""

import concurrent.futures as cf
import multiprocessing as mp
import os
import time
from dataclasses import dataclass
from io import BytesIO

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import xarray as xr
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource

from forcingprocessor.utils import get_window

gpd.options.io_engine = "pyogrio"

# Full CONUS NWM forcing grid.
CONUS_NX = 4608
CONUS_NY = 3840


@dataclass(frozen=True)
class Window:
    """The grid subset a weight table falls inside. Inclusive on both ends."""

    x_min: int
    x_max: int
    y_min: int
    y_max: int

    @property
    def nx(self) -> int:
        """Width of the window in grid cells."""
        return self.x_max - self.x_min + 1

    @property
    def ny(self) -> int:
        """Height of the window in grid cells."""
        return self.y_max - self.y_min + 1


def grid_window(weights_df: pd.DataFrame) -> Window:
    """Smallest grid window containing every cell in a weight table.

    Args:
        weights_df (pd.DataFrame): Weight table indexed by catchment id with cell_id and coverage
            columns.

    Returns:
        Window: The grid subset the table falls inside.
    """
    x_min, x_max, y_min, y_max = get_window(weights_df)
    return Window(
        x_min=int(x_min), x_max=int(x_max), y_min=int(y_min), y_max=int(y_max)
    )


def normalize_weight_table(weights_df: pd.DataFrame) -> pd.DataFrame:
    """Coerce a hydrofabric forcing-weights table into the WeightTable shape.

    Long tables keyed on 'cell' carry one row per grid cell and are grouped down to one row per
    divide. Tables already in cell_id/coverage form pass through untouched.

    Args:
        weights_df (pd.DataFrame): Weight table in either the long or the grouped form.

    Returns:
        pd.DataFrame: A weight table indexed by divide_id with cell_id and coverage columns.
    """
    if "cell" not in weights_df.columns:
        return weights_df

    weights_table_unqiue_ids = (
        weights_df.groupby("divide_id").agg(tuple).map(list).reset_index()
    )
    weights_table_unqiue_ids = weights_table_unqiue_ids.set_index("divide_id")
    weights_df = weights_table_unqiue_ids.rename(columns={"cell": "cell_id"})
    weights_df["cell_id"] = weights_df["cell_id"].apply(lambda x: [int(i) for i in x])
    return weights_df.rename(columns={"coverage_fraction": "coverage"})


def _rastersourceNexactextract(
    raster_data: xr.Dataset, geo_data: gpd.GeoDataFrame
) -> pd.DataFrame | None:
    ncatch_proc = len(geo_data)

    print(f"Finding weights for geodataframe of size {ncatch_proc}", flush=True)
    xmin = raster_data.x[0]
    xmax = raster_data.x[-1]
    ymin = raster_data.y[0]
    ymax = raster_data.y[-1]
    t0 = time.perf_counter()
    rastersource = NumPyRasterSource(
        np.squeeze(raster_data["T2D"]),
        srs_wkt=geo_data.crs.to_wkt(),  # type: ignore
        xmin=xmin,
        xmax=xmax,
        ymin=ymin,
        ymax=ymax,
    )
    print("raster calculated, executing exactextract", flush=True)
    output = exact_extract(
        rastersource,
        geo_data,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )
    tf = time.perf_counter() - t0
    assert ncatch_proc == len(output)  # type: ignore
    print(
        f"single thread -> {ncatch_proc} weights calculated in {tf:.1f}s for a rate of "
        + f"{ncatch_proc / tf:.1f}catch/s",
        flush=True,
    )

    return output  # type: ignore


def _get_projection(raster_filepath: str) -> tuple[str, xr.Dataset]:
    if "https://" in raster_filepath:
        print("Downloading file...")
        response = requests.get(raster_filepath, timeout=10)

        if response.status_code == 200:
            raster_file = BytesIO(response.content)
        else:
            raster_file = raster_filepath
    else:
        raster_file = raster_filepath

    print("Opening raster", flush=True)
    try:
        raster_data = xr.open_dataset(raster_file)
        print("Attemping Projection", flush=True)
        projection = raster_data.crs.esri_pe_string
        print("Projection successful")
    except Exception as exc:
        raster_backup = (
            "https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/CONUS/netcdf/"
            + "FORCING/2018/201801010000.LDASIN_DOMAIN1"
        )
        if raster_backup == raster_file:
            raise RuntimeError("Projection failed") from exc
        print(
            f"No projection found in {raster_file}\nSwitching to template file: {raster_backup}"
        )
        projection, raster_data = _get_projection(raster_backup)

    return projection, raster_data


def calc_weights_from_gdf(
    gdf: gpd.GeoDataFrame, raster_file: str, nf: int
) -> pd.DataFrame:
    """Create a dict of weights from the "divides" layer geodataframe keys are divide_ids, values
    are a 2 element list with the first element being a list of cell_id's and the second element
    being the corresponding coverage fraction's

    This is the coverage fraction scheme forcingprocessor ships with: every grid cell a catchment
    touches, weighted by the fraction of that cell inside the catchment, via exactextract.

    Args:
        gdf (gpd.GeoDataFrame): Geodataframe containing the catchment geometries.
        raster_file (str): Path to the raster file.
        nf (int): Number of files to process.

    Returns:
        pd.DataFrame: A dataframe where index is catchment ids and the columns are the corresponding
            cell and coverage
    """

    projection, raster_data = _get_projection(raster_file)
    geo_data = gdf.to_crs(projection)
    nrows = len(gdf)

    cpu_count = os.cpu_count()
    if cpu_count is None:
        cpu_count = 1

    nprocs = max(min(nrows // 9000, (cpu_count - 1) // nf), 1)
    geo_df_list = []
    nper = nrows // nprocs
    nleft = nrows - (nper * nprocs)
    i = 0
    k = nper
    for j in range(nprocs):
        if j < nleft:
            k += 1
        print(f"{i} {k} {k - i}")
        geo_df_list.append(geo_data[i:k])
        i = k
        k = nper + i

    print("Performing multiprocess exactextract", flush=True)
    raster_list = [raster_data for x in range(nprocs)]
    with cf.ProcessPoolExecutor(
        max_workers=nprocs,
        mp_context=mp.get_context("spawn"),
    ) as pool:
        output_list = list(
            pool.map(_rastersourceNexactextract, raster_list, geo_df_list)
        )
    print("Concatenating results", flush=True)
    output = pd.concat(output_list, ignore_index=True)
    weights = output.set_index("divide_id")
    return weights
