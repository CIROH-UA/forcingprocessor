import json
import argparse
import time
import os
from io import BytesIO
from typing import Tuple
import concurrent.futures as cf
import multiprocessing as mp

import requests
import geopandas as gpd
import pandas as pd
import xarray as xr
import numpy as np
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource

from forcingprocessor.utils import normalize_vpu_id
gpd.options.io_engine = "pyogrio"



def rastersourceNexactextract(raster_data, geo_data):

    ncatch_proc = len(geo_data)

    print(f"Finding weights for geodataframe of size {ncatch_proc}", flush=True)
    xmin = raster_data.x[0]
    xmax = raster_data.x[-1]
    ymin = raster_data.y[0]
    ymax = raster_data.y[-1]
    # print(f"window {xmin.value} {xmax.value} {ymin.value} {ymax.value}")
    t0 = time.perf_counter()
    rastersource = NumPyRasterSource(
        np.squeeze(raster_data["T2D"]),
        srs_wkt=geo_data.crs.to_wkt(),
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
    assert ncatch_proc == len(output) # type: ignore
    print(
        f"single thread -> {ncatch_proc} weights calculated in {tf:.1f}s for a rate of " +
        f"{ncatch_proc / tf:.1f}catch/s",
        flush=True,
    )

    return output


def get_projection(raster_file):
    if "https://" in raster_file:
        print("Downloading file...")
        response = requests.get(raster_file, timeout=10)

        if response.status_code == 200:
            raster_file = BytesIO(response.content)

    print("Opening raster", flush=True)
    try:
        raster_data = xr.open_dataset(raster_file)
        print("Attemping Projection", flush=True)
        projection = raster_data.crs.esri_pe_string
        print("Projection successful")
    except:
        raster_backup = (
            "https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/CONUS/netcdf/" +
            "FORCING/2018/201801010000.LDASIN_DOMAIN1"
        )
        if raster_backup == raster_file:
            raise Exception("Projection failed")
        print(
            f"No projection found in {raster_file}\nSwitching to template file: {raster_backup}"
        )
        projection, raster_data = get_projection(raster_backup)

    return projection, raster_data


def calc_weights_from_gdf(gdf: gpd.GeoDataFrame, raster_file: str, nf: str) -> pd.DataFrame:
    # Create a dict of weights from the "divides" layer geodataframe
    # keys are divide_ids, values are a 2 element list
    # with the first element being a list of cell_id's
    # and the second element being the corresponding coverage fraction's
    projection, raster_data = get_projection(raster_file)
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
    output_list = []
    raster_list = [raster_data for x in range(nprocs)]
    with cf.ProcessPoolExecutor(
        max_workers=nprocs,
        mp_context=mp.get_context("spawn"),
    ) as pool:
        for results in pool.map(rastersourceNexactextract, raster_list, geo_df_list):
            output_list.append(results)
    print("Concatenating results", flush=True)
    output = pd.concat(output_list, ignore_index=True)
    weights = output.set_index("divide_id")
    return weights


def multiprocess_hf2ds(
        files: list,
        raster_template: str,
        max_procs: int
    ) -> Tuple[pd.DataFrame, dict]:

    nprocs = min(len(files), max_procs)
    nf = len(files)
    files_list = []
    nper = nf // nprocs
    nleft = nf - (nper * nprocs)
    i = 0
    k = nper
    for j in range(nprocs):
        if j < nleft:
            k += 1
        files_list.append(files[i:k])
        i = k
        k = nper + i

    weight_dfs = []
    jcatchment_dicts = []
    with cf.ProcessPoolExecutor(
        max_workers=nprocs,
        mp_context=mp.get_context("spawn"),
    ) as pool:
        for results in pool.map(
            hf2ds,
            files_list,
            [raster_template for x in range(len(files_list))],
            [nf for x in range(len(files_list))],
        ):
            weight_dfs.append(results[0])
            jcatchment_dicts.append(results[1])

    weights_df = pd.concat(weight_dfs)

    print("Processes have returned", flush=True)

    jcatchment_dict = {}

    for process_dict in jcatchment_dicts:
        for key, catchments in process_dict.items():
            unique_key = key
            suffix = 1

            while unique_key in jcatchment_dict:
                unique_key = f"{key}_{suffix}"
                suffix += 1

            jcatchment_dict[unique_key] = catchments

    return weights_df, jcatchment_dict


def hf2ds(files: list, raster: str, nf) -> Tuple[pd.DataFrame, dict]:
    """
    Extracts the weights from a list of files

    input : files
    gpkg_files : list of geopackage or parquet files

    returns : weights_df, jcatchment_dict
    weights_df : a dataframe where index is catchment ids and the columns are the corresponding cell
        and coverage
    jcatchment_dict : A dictionary where the keys are the geopackage name and the values are a list
        of catchment id's

    """
    jcatchment_dict = {}
    count = 0
    weights_dfs = []
    for jgpkg in files:
        jname = normalize_vpu_id(jgpkg)
        if jname in jcatchment_dict:
            count += 1
            jname = f"{jname}_{count}"

        jweights_df = hydrofabric2datastream_weights(jgpkg, raster, nf)
        weights_dfs.append(jweights_df)
        jcatchment_dict[jname] = list(jweights_df.index)

    weights_df = pd.concat(weights_dfs)

    return weights_df, jcatchment_dict


def hydrofabric2datastream_weights(
    weights_file: str, raster_template: str, nf: int
) -> pd.DataFrame:
    """
    Converts tabular weights to a dictionary where keys are catchment ids and the values are a list
    of weights

    input gpkg or path to weights parquet
    gpkg : gpd.Dataframe

    returns weights_json : a dictionary where keys are catchment ids and the values are a list of
        weights

    """
    # This function looks a bit wild bc weights may be provided
    # to datastream in several different ways, or not at all.
    # Need to handle each situation.

    t0 = time.perf_counter()

    weights_file = str(weights_file)

    if weights_file.endswith(".json"):
        with open(weights_file, "r", encoding="utf-8") as fp:
            weights_json = json.load(fp)
        ncatchment = len(weights_json)
        weights_df = pd.DataFrame.from_dict(
            weights_json, orient="index", columns=["cell_id", "coverage"]
        )
    else:
        if weights_file.endswith(".gpkg"):
            catchments = gpd.read_file(weights_file, layer="divides")
            layers = gpd.list_layers(weights_file)
            if "forcing-weights" in list(layers.name):
                print(
                    "Weights table found in geopackage as 'forcing-weights'. Converting to dict " +
                    "for processing.",
                    flush=True,
                )
                weights_df = gpd.read_file(weights_file, layer="forcing-weights")
            else:
                print(
                    "Weights table not found in geopackage. Calculating from scratch with raster " +
                    f"{raster_template}.",
                    flush=True,
                )
                weights_df = calc_weights_from_gdf(catchments, raster_template, nf)
                ncatchment = len(weights_df)
        elif weights_file.endswith("parquet"):
            weights_df = pd.read_parquet(weights_file)
            ncatchment = len(weights_df)
        else:
            raise ValueError(f"Dont know how to deal with {weights_file}")

        if "cell" in weights_df.columns:
            weights_table_unqiue_ids = (
                weights_df.groupby("divide_id").agg(tuple).map(list).reset_index()
            )
            weights_table_unqiue_ids = weights_table_unqiue_ids.set_index("divide_id")
            weights_df = weights_table_unqiue_ids.rename(columns={"cell": "cell_id"})
            weights_df["cell_id"] = weights_df["cell_id"].apply(
                lambda x: [int(i) for i in x]
            )
            weights_df = weights_df.rename(columns={"coverage_fraction": "coverage"})
            ncatchment = len(weights_df)

    ncatchment = len(weights_df)
    tf = time.perf_counter()
    dt = tf - t0
    rate = ncatchment / dt if dt > 0 else float("inf")
    print(
        f"{weights_file} {ncatchment} catchment weights obtained {dt:.2f} seconds total, " +
        f"{rate:.2f} catchments/second",
        flush=True,
    )
    return weights_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        dest="input_file",
        type=str,
        help="Path to geopackage or weights parquet file",
        default=None,
    )
    parser.add_argument(
        "--outname",
        dest="outname",
        type=str,
        help="Filename for the datastream weights file",
    )
    args = parser.parse_args()

    RASTER_TEMPLATE = (
        "https://noaa-nwm-pds.s3.amazonaws.com/nwm.20250105/forcing_short_range/" +
        "nwm.t00z.short_range.forcing.f001.conus.nc"
    )

    weights_to_write, jcatchments = hf2ds([args.input_file], RASTER_TEMPLATE, 1)
    weights_to_write.to_parquet(args.outname)
