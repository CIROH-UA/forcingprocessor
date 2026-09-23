"""Utility functions for hydrofabric catchment weights.

Weights may reach forcingprocessor several different ways: already tabulated inside a geopackage,
as a standalone parquet or json, or not at all. This module's job is to produce a weight table
however it has to. When there is nothing to load it calls the generation kernel in weights.py.
"""

import argparse
import concurrent.futures as cf
import json
import multiprocessing as mp
import time

import geopandas as gpd
import pandas as pd

from forcingprocessor.utils import normalize_vpu_id
from forcingprocessor.weights import calc_weights_from_gdf, normalize_weight_table

gpd.options.io_engine = "pyogrio"

__all__ = [
    "calc_weights_from_gdf",
    "hf2ds",
    "multiprocess_hf2ds",
]


def multiprocess_hf2ds(
    files: list, raster_template: str, max_procs: int
) -> tuple[pd.DataFrame, dict]:
    """Parallelized weights extraction from a list of files.

    Args:
        files (list): List of files to process.
        raster_template (str): Template raster file.
        max_procs (int): Maximum number of processes to use.

    Returns:
        Tuple[pd.DataFrame, dict]: Extracted weights and catchment dictionary.
    """
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


def hf2ds(
    files: list, raster: str | None = None, nf: int = 1
) -> tuple[pd.DataFrame, dict]:
    """
    Extracts the weights from a list of files

    Args:
        files (list): List of geopackage or parquet files to process.
        raster (str | None): Path to the raster file. Only needed when a source carries no weights
            table and they have to be calculated. Defaults to None.
        nf (int): Number of files to process. Defaults to 1.

    Returns:
        Tuple[pd.DataFrame, dict]:
            weights_df : a dataframe where index is catchment ids and the columns are the
                corresponding cell and coverage
            jcatchment_dict : A dictionary where the keys are the geopackage name and the values are
                a list of catchment id's
    """
    jcatchment_dict = {}
    count = 0
    weights_dfs = []
    for jgpkg in files:
        jname = normalize_vpu_id(jgpkg)
        if jname in jcatchment_dict:
            count += 1
            jname = f"{jname}_{count}"

        jweights_df = _hydrofabric2datastream_weights(jgpkg, raster, nf)
        weights_dfs.append(jweights_df)
        jcatchment_dict[jname] = list(jweights_df.index)

    weights_df = pd.concat(weights_dfs)

    return weights_df, jcatchment_dict


def _hydrofabric2datastream_weights(
    weights_file: str, raster_template: str | None = None, nf: int = 1
) -> pd.DataFrame:
    """
    Converts tabular weights to a dataframe where the index is catchment ids and the values are the
    corresponding cells and coverages.

    Args:
        weights_file (str): Path to the weights file (geopackage, parquet or json).
        raster_template (str | None): Path to the raster file. Only needed when the source carries
            no weights table. Defaults to None.
        nf (int): Number of files to process. Defaults to 1.

    Raises:
        ValueError: Raised for an unrecognized source, or when weights must be calculated but no
            raster_template was supplied.

    Returns:
        pd.DataFrame: A dataframe where index is catchment ids and the columns are the corresponding
            cell and coverage
    """
    # This function looks a bit wild bc weights may be provided
    # to datastream in several different ways, or not at all.
    # Need to handle each situation.

    t0 = time.perf_counter()

    weights_file = str(weights_file)

    if weights_file.endswith(".json"):
        with open(weights_file, "r", encoding="utf-8") as fp:
            weights_json = json.load(fp)
        weights_df = pd.DataFrame.from_dict(
            weights_json, orient="index", columns=["cell_id", "coverage"]
        )
    elif weights_file.endswith(".gpkg"):
        layers = gpd.list_layers(weights_file)
        if "forcing-weights" in list(layers.name):
            print(
                "Weights table found in geopackage as 'forcing-weights'. Converting to dict "
                + "for processing.",
                flush=True,
            )
            weights_df = normalize_weight_table(
                gpd.read_file(weights_file, layer="forcing-weights")
            )
        elif raster_template is None:
            raise ValueError(
                f"{weights_file} carries no weights table, so a raster_template is required "
                + "to calculate them"
            )
        else:
            print(
                "Weights table not found in geopackage. Calculating from scratch with raster "
                + f"{raster_template}.",
                flush=True,
            )
            catchments = gpd.read_file(weights_file, layer="divides")
            weights_df = calc_weights_from_gdf(catchments, raster_template, nf)
    elif weights_file.endswith("parquet"):
        weights_df = normalize_weight_table(pd.read_parquet(weights_file))
    else:
        raise ValueError(f"Dont know how to deal with {weights_file}")

    ncatchment = len(weights_df)
    dt = time.perf_counter() - t0
    rate = ncatchment / dt if dt > 0 else float("inf")
    print(
        f"{weights_file} {ncatchment} catchment weights obtained {dt:.2f} seconds total, "
        + f"{rate:.2f} catchments/second",
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
        "https://noaa-nwm-pds.s3.amazonaws.com/nwm.20250105/forcing_short_range/"
        + "nwm.t00z.short_range.forcing.f001.conus.nc"
    )

    weights_to_write, jcatchments = hf2ds([args.input_file], RASTER_TEMPLATE, 1)
    weights_to_write.to_parquet(args.outname)
