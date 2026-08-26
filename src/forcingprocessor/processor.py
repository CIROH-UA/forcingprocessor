import argparse
import concurrent.futures as cf
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path

import gcsfs
import geopandas as gpd
import numpy as np
import requests
import s3fs
import xarray as xr
import pandas as pd

from forcingprocessor.channel_routing_tools import (
    channelrouting_nwm2ngen,
    write_netcdf_chrt,
)
from forcingprocessor.config import (
    build_output_layout,
    parse_nwm_filenames,
    read_config,
    write_run_manifest,
    RunConfig,
)
from forcingprocessor.metadata import collect_metadata
from forcingprocessor.plot_forcings import plot_ngen_forcings
from forcingprocessor.troute_restart_tools import create_restart, write_netcdf_restart
from forcingprocessor.utils import (
    B2MB,
    convert_url2key,
    distribute_work,
    get_window,
    load_balance,
    ngen_variables,
    nwm_variables,
    phase,
    Profiler,
    read_dataset,
    read_json,
    report_usage,
)
from forcingprocessor.weights_hf2ds import multiprocess_hf2ds
from forcingprocessor.writers import (
    multiprocess_write_df,
    multiprocess_write_netcdf,
    multiprocess_write_tar,
)


@dataclass
class Geometry:
    """Catchment geometry and identifier mappings the run extracts against."""

    ncatchments: int = 0
    weights_df: pd.DataFrame | None = None
    jcatchment_dict: dict | None = None
    window: list | None = None
    nwm_ngen_map: dict | None = None
    cat_map: dict | None = None
    crosswalk_ds: xr.Dataset | None = None
    routelink_ds: xr.Dataset | None = None


@dataclass
class Extracted:
    """Data pulled out of the NWM files, ordered in time."""

    data_array: xr.Dataset | np.ndarray | None
    t_ax: list | None = None
    nwm_data: object = None
    nwm_file_sizes_MB: list | None = None

    def release(self):
        """Clear NWM data from memory."""
        self.data_array = None


@dataclass
class WriteResult:
    """Identifiers and file sizes reported back by the write processes."""

    forcing_cat_ids: list | None = None
    filenames: list | None = None
    cat_file_sizes_MB: list | None = None
    cat_file_sizes_zipped_MB: list | None = None
    tar_buffs: list | None = None
    netcdf_file_sizes_MB: list | None = None


def pool_filesystem(fs_type: str) -> s3fs.S3FileSystem | str | None:
    """Filesystem handed to the extraction pool. Google is deferred to the workers
    because a GCSFileSystem does not survive the trip to a subprocess.

    Args:
        fs_type (str): String describing filesystem type
    Returns:
        s3fs.S3FileSystem | str | None: S3FileSystem for S3, "google" for GCS, None for local.
    """
    if fs_type == "s3":
        return s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": "us-east-1"})
    if fs_type == "google":
        return "google"
    return None


def multiprocess_data_extract(
    cfg: RunConfig,
    files: list[str],
    weights_df: pd.DataFrame,
    window: list[float],
    fs: s3fs.S3FileSystem | str | None,
) -> tuple[
    np.ndarray[tuple[int, int, int], np.dtype[np.float64]],
    list[str],
    np.ndarray[tuple[int, int, int], np.dtype[np.float64]],
    list[float],
]:
    """
    Sets up the multiprocessing pool for forcing_grid2catchment and returns the data and time axis
    ordered in time.

    Parameters:
        cfg (RunConfig): Run configuration.
        files (list[str]): List of files to be processed.
        weights_df (pd.DataFrame): DataFrame containing catchment weights.
        window (list[float]): Grid window the weights fall inside.
        fs (s3fs.S3FileSystem | str | None): s3fs.S3FileSystem for S3, "google" for GCS, None for
            local

    Returns:
        data_array (np.ndarray[tuple[int, int, int], np.dtype[np.float64]]): Concatenated array
            containing the extracted data.
        t_ax_local (list[str]): List of time axes corresponding to the extracted data.
        nwm_data (np.ndarray[tuple[int, int, int], np.dtype[np.float64]]): Extracted data to be
            plotted.
        nwm_file_sizes_out (list[float]): List of sizes of NWM files read in MB.
    """
    files_per_proc = distribute_work(files, cfg.nprocs)
    files_per_proc = load_balance(files_per_proc, cfg.ii_verbose)
    nprocs = len(files_per_proc)

    start = 0
    nfiles = len(files)
    files_list = []
    for i in range(nprocs):
        end = min(start + files_per_proc[i], nfiles)
        files_list.append(files[start:end])
        start = end

    data_ax = []
    t_ax_local = []
    nwm_data = []
    nwm_file_sizes = []
    with cf.ProcessPoolExecutor(max_workers=nprocs) as pool:
        for results in pool.map(
            forcing_grid2catchment,
            files_list,
            [fs for x in range(nprocs)],
            [cfg.ngen_vars_plot for x in range(nprocs)],
            [weights_df for x in range(nprocs)],
            [window for x in range(nprocs)],
            [cfg.fs_type for x in range(nprocs)],
            [cfg.ii_verbose for x in range(nprocs)],
            [cfg.ii_plot for x in range(nprocs)],
            [cfg.nts_plot for x in range(nprocs)],
        ):
            data_ax.append(results[0])
            t_ax_local.append(results[1])
            nwm_data.append(results[2])
            nwm_file_sizes.append(results[3])

    print("Processes have returned")
    del weights_df
    data_array = np.concatenate(data_ax)
    t_ax_local = [item for sublist in t_ax_local for item in sublist]
    nwm_file_sizes_out = [item for sublist in nwm_file_sizes for item in sublist]
    nwm_data = np.concatenate(nwm_data)

    return data_array, t_ax_local, nwm_data, nwm_file_sizes_out


def multiprocess_chrt_extract(
    cfg: RunConfig,
    files: list[str],
    mapping: dict[str, list[str]],
    fs: s3fs.S3FileSystem | str | None,
) -> tuple[np.ndarray[tuple[int], np.dtype[np.float64]], list[str], list[float]]:
    """
    Sets up the multiprocessing pool for forcing_grid2catchment and returns the data and time axis
    ordered in time.

    Parameters:
        cfg (RunConfig): Run configuration.
        files (list[str]): List of files to be processed.
        mapping (dict[str, list[str]]): Dictionary that maps NWM IDs to NGEN IDs.
        fs (s3fs.S3FileSystem | str | None): Filesystem for cloud storage reads. s3fs.S3FileSystem
            for S3, "google" for GCS, None for local

    Returns:
        data_array (np.ndarray[tuple[int], np.dtype[np.float64]]): Concatenated array containing the
            extracted data.
        t_ax_local (list[str]): List of time axes corresponding to the extracted data.
        nwm_file_sizes_out (list[float]): List of file sizes of each input CHRTOUT file.
    """
    files_per_proc = distribute_work(files, cfg.nprocs)
    files_per_proc = load_balance(files_per_proc, cfg.ii_verbose)
    num_procs = len(files_per_proc)

    start = 0
    nfiles = len(files)
    files_list = []
    for i in range(num_procs):
        end = min(start + files_per_proc[i], nfiles)
        files_list.append(files[start:end])
        start = end

    data_ax = []
    t_ax_local = []
    nwm_file_sizes = []
    with cf.ProcessPoolExecutor(max_workers=num_procs) as pool:
        for results in pool.map(
            channelrouting_nwm2ngen,
            files_list,
            [mapping for x in range(num_procs)],
            [cfg.fs_type for x in range(num_procs)],
            [fs for x in range(num_procs)],
            [cfg.ii_verbose for x in range(num_procs)],
        ):
            data_ax.append(results[0])
            t_ax_local.append(results[1])
            nwm_file_sizes.append(results[2])

    print("Processes have returned")
    data_array_temp = np.concatenate(data_ax)
    data_array = data_array_temp.copy().astype(object)
    data_array[:, :, 1] = data_array[:, :, 1].astype(float)

    t_ax_local = [item for sublist in t_ax_local for item in sublist]
    nwm_file_sizes_out = [item for sublist in nwm_file_sizes for item in sublist]

    return data_array, t_ax_local, nwm_file_sizes_out


def forcing_grid2catchment(
    nwm_files: list[str],
    fs: s3fs.S3FileSystem | gcsfs.GCSFileSystem | None = None,
    ngen_vars_plot: list[str] | None =None,
    weights_df: pd.DataFrame | None = None,
    window: list[float] | None =None,
    fs_type: str | None = None,
    ii_verbose: bool = False,
    ii_plot: bool = False,
    nts_plot: int = 1,
) -> list[list[np.ndarray | str | float]]:
    """
    Retrieve catchment level data from national water model files

    Inputs:
    nwm_files: list of filenames (urls for remote, local paths otherwise),
    fs: an optional file system for cloud storage reads
    ngen_variables: List of variables to read out of the nwm netcdf
    ngen_vars_plot: List of ngen variables to plot
    weights_df: dataframe of weights. weights are values 0-1 corresponding to the percentage an
        overlapping a grid point on a polygon
    fs_type: type of file system
    ii_verbose: verbosity
    ii_plot: save data for plotting
    nts_plot: number of time steps to include in gif

    Outputs: [data_list, t_list, nwm_data]
    data_list : list of ngen forcings ordered in time.
    ngen_forcings : 2d darray (forcing_variable x catchment)
    t : model_output_valid_time for each
    nwm_data : nwm data saved for plotting.
    nwm_data : 3d array (forcing_variable x west_east x south_north)
    """
    if ngen_vars_plot is None:
        ngen_vars_plot = []
    if window is None:
        window = []
    if weights_df is None:
        weights_df = pd.DataFrame()

    topen = 0
    txrds = 0
    tfill = 0
    tdata = 0
    t_list = []
    nwm_data_plot = []
    jplot_vars = np.array(
        [x for x in range(len(ngen_variables)) if ngen_variables[x] in ngen_vars_plot]
    )
    nfiles = len(nwm_files)
    nvar = len(nwm_variables)

    x_max = window[0]
    x_min = window[1]
    y_max = window[2]
    y_min = window[3]

    dx = x_max - x_min + 1
    dy = y_max - y_min + 1

    if fs_type == "google":
        fs = gcsfs.GCSFileSystem()
    pid = os.getpid()
    if ii_verbose:
        print(
            f"Process #{pid} extracting data from {nfiles} files", end=None, flush=True
        )
    data_list = []
    nwm_file_sizes_MB = []
    for j, nwm_file in enumerate(nwm_files):
        t0 = time.perf_counter()
        if fs:
            if nwm_file.find("https://") >= 0:
                _, bucket_key = convert_url2key(nwm_file, fs_type)
            else:
                bucket_key = nwm_file
            file_obj = fs.open(bucket_key, mode="rb")
            nwm_file_sizes_MB.append(file_obj.details["size"])  # type: ignore
        elif "https://" in nwm_file:
            response = requests.get(nwm_file, timeout=10)

            if response.status_code == 200:
                file_obj = BytesIO(response.content)
            else:
                raise FileNotFoundError(f"{nwm_file} does not exist")
            nwm_file_sizes_MB.append(len(response.content) / B2MB)
        else:
            file_obj = nwm_file
            nwm_file_sizes_MB.append(os.path.getsize(nwm_file) / B2MB)

        topen += time.perf_counter() - t0
        t0 = time.perf_counter()
        with xr.open_dataset(file_obj) as nwm_data:
            txrds += time.perf_counter() - t0
            t0 = time.perf_counter()
            shp = nwm_data["U2D"].shape
            data_allvars = np.zeros(shape=(nvar, dy, dx), dtype=np.float64) # type: ignore
            for var_dx, jvar in enumerate(nwm_variables):
                if "retrospective-2-1" in nwm_file or (
                    "south_north" in nwm_data.dims and "west_east" in nwm_data.dims
                ):
                    data_allvars[var_dx, :, :] = np.flip(
                        np.squeeze(
                            nwm_data[jvar]
                            .isel(
                                west_east=slice(x_min, x_max + 1),
                                south_north=slice(shp[1] - (y_max + 1), shp[1] - y_min),
                            )
                            .values
                        ),
                        axis=0,
                    )
                    t = datetime.strftime(
                        datetime.strptime(
                            nwm_file.split("/")[-1].split(".")[0], "%Y%m%d%H"
                        ),
                        "%Y-%m-%d %H:%M:%S",
                    )
                else:
                    data_allvars[var_dx, :, :] = np.flip(
                        np.squeeze(
                            nwm_data[jvar]
                            .isel(
                                x=slice(x_min, x_max + 1),
                                y=slice(shp[1] - (y_max + 1), shp[1] - y_min),
                            )
                            .values
                        ),
                        axis=0,
                    )
                    time_splt = nwm_data.attrs["model_output_valid_time"].split("_")
                    t = time_splt[0] + " " + time_splt[1]
            t_list.append(t)
            if ii_plot and j < nts_plot:
                nwm_data_plot.append(data_allvars[jplot_vars, :, :])
        del nwm_data
        tfill += time.perf_counter() - t0

        t0 = time.perf_counter()
        data_allvars = data_allvars.reshape(nvar, dx * dy)
        ncatch = len(weights_df)
        data_array = np.zeros((nvar, ncatch), dtype=np.float64)
        jcatch = 0
        for row in weights_df.itertuples():
            weights = row.cell_id
            coverage = np.array(row.coverage)
            coverage_mat = np.repeat(coverage[None, :], nvar, axis=0)

            ( # pylint: disable=unbalanced-tuple-unpacking
                weights_dx,
                weights_dy,
            ) = np.unravel_index(
                weights, (shp[2], shp[1]), order="F"  # type: ignore
            )
            weights_dx_shifted = list(weights_dx - x_min)
            weights_dy_shifted = list(weights_dy - y_min)
            weights_window = np.ravel_multi_index(
                np.array([weights_dx_shifted, weights_dy_shifted]), (dx, dy), order="F" # type: ignore
            )
            jcatch_data_mask = data_allvars[:, weights_window]

            weight_sum = np.sum(coverage)
            data_array[:, jcatch] = (
                np.sum(coverage_mat * jcatch_data_mask, axis=1) / weight_sum
            )
            jcatch += 1

        del data_allvars
        data_list.append(data_array)
        tdata += time.perf_counter() - t0
        ttotal = topen + txrds + tfill + tdata
        if ii_verbose:
            print(
                f"\nAverage time for:\nfs open file: {topen / (j + 1):.2f} s"
                + f"\nxarray open dataset: {txrds / (j + 1):.2f} s"
                + f"\nfill array: {tfill / (j + 1):.2f} s"
                + f"\ncalculate catchment values: {tdata / (j + 1):.2f} s"
                + f"\ntotal {ttotal / (j + 1):.2f} s\npercent complete {100 * (j + 1) / nfiles:.2f}",
                end=None,
                flush=True,
            )
        report_usage()

    if ii_verbose:
        print(
            f"Process #{pid} completed data extraction, returning data to primary process",
            flush=True,
        )
    return [data_list, t_list, nwm_data_plot, nwm_file_sizes_MB]


def load_geometry(cfg, profiler):
    """
    Read the catchment weights, nexus map, or restart mappings this run extracts against.
    """
    if cfg.data_source == "forcings":
        with phase("READWEIGHTS", profiler):
            if cfg.ii_verbose:
                print("Obtaining weights\n", flush=True)
            weights_df, jcatchment_dict = multiprocess_hf2ds(
                cfg.gpkg_files, cfg.nwm_forcing_files[0], cfg.nprocs
            )
        with phase("CALC_WINDOW", profiler):
            x_min, x_max, y_min, y_max = get_window(weights_df)
        return Geometry(
            ncatchments=len(weights_df),
            weights_df=weights_df,
            jcatchment_dict=jcatchment_dict,
            window=[x_max, x_min, y_max, y_min],
        )

    if cfg.data_source == "channel_routing":
        with phase("READMAP", profiler):
            if cfg.ii_verbose:
                print("Reading NWM to NGEN map\n", flush=True)
            full_nwm_ngen_map = read_json(cfg.map_file)
            catchments = gpd.read_file(cfg.gpkg_files[0], layer="nexus")["id"].to_list()
            nwm_ngen_map = {
                jcatch: full_nwm_ngen_map[jcatch]
                for jcatch in catchments
                if not any(x in jcatch for x in ["tnx", "cnx", "inx"])
            }
        return Geometry(ncatchments=len(nwm_ngen_map), nwm_ngen_map=nwm_ngen_map)

    return Geometry(
        ncatchments=1,
        cat_map=read_json(cfg.restart_map_file),
        crosswalk_ds=read_dataset(cfg.crosswalk_file),
        routelink_ds=read_dataset(cfg.routelink_file),
    )


def extract_restart(cfg, geom):
    """_summary_

    Args:
        cfg (_type_): _description_
        geom (_type_): _description_

    Raises:
        RuntimeError: _description_

    Returns:
        _type_: _description_
    """
    nwm_file = cfg.nwm_forcing_files[0]
    nwm_file_sizes_MB = []
    if cfg.fs_type == "google":
        fs = gcsfs.GCSFileSystem()
    elif cfg.fs_type == "s3":
        fs = s3fs.S3FileSystem(anon=True)
    else:
        fs = None

    if fs:
        if nwm_file.find("https://") >= 0:
            _, bucket_key = convert_url2key(nwm_file, cfg.fs_type)
        else:
            bucket_key = nwm_file
        file_obj = fs.open(bucket_key, mode="rb")
        nwm_file_sizes_MB.append(file_obj.details["size"])  # type: ignore
    elif "https://" in nwm_file:
        response = requests.get(nwm_file, timeout=10)

        if response.status_code == 200:
            file_obj = BytesIO(response.content)
        else:
            raise RuntimeError(f"{nwm_file} does not exist")
        nwm_file_sizes_MB.append(len(response.content) / B2MB)
    else:
        file_obj = nwm_file
        nwm_file_sizes_MB.append(os.path.getsize(nwm_file) / B2MB)

    nwm_ds = xr.open_dataset(file_obj).load()
    data_array = create_restart(
        geom.cat_map, geom.crosswalk_ds, nwm_ds, geom.routelink_ds
    )
    return Extracted(data_array=data_array, nwm_file_sizes_MB=nwm_file_sizes_MB)


def extract(cfg, geom, nwm_meta, profiler):
    """
    Pull the requested data out of the NWM files, ordered so time moves forward.
    """
    with phase("PROCESSING", profiler):
        if cfg.ii_verbose:
            print("Entering data extraction...\n", flush=True)

        if cfg.data_source == "troute_restarts":
            return extract_restart(cfg, geom)

        fs = pool_filesystem(cfg.fs_type)
        if cfg.data_source == "forcings":
            data_array, t_ax, nwm_data, sizes = multiprocess_data_extract(
                cfg, cfg.nwm_forcing_files, geom.weights_df, geom.window, fs
            )
        else:
            nwm_data = None
            data_array, t_ax, sizes = multiprocess_chrt_extract(
                cfg, cfg.nwm_forcing_files, geom.nwm_ngen_map, fs
            )

        if datetime.strptime(t_ax[0], "%Y-%m-%d %H:%M:%S") > datetime.strptime(
            t_ax[-1], "%Y-%m-%d %H:%M:%S"
        ):
            # Hack to ensure data is always written out with time moving forward.
            t_ax = list(reversed(t_ax))
            data_array = np.flip(data_array, axis=0)
            nwm_meta.lead_start, nwm_meta.lead_end = (
                nwm_meta.lead_end,
                nwm_meta.lead_start,
            )

    if cfg.ii_verbose:
        t_extract = profiler.timings["PROCESSING"]
        complexity = (len(cfg.nwm_forcing_files) * geom.ncatchments) / 10000
        print(
            f"Data extract processs: {cfg.nprocs:.2f}\nExtract time: {t_extract:.2f}"
            + f"\nComplexity: {complexity:.2f}\nScore: {complexity / t_extract:.2f}\n",
            end=None,
            flush=True,
        )

    return Extracted(
        data_array=data_array,
        t_ax=t_ax,
        nwm_data=nwm_data,
        nwm_file_sizes_MB=sizes,
    )


def write_netcdf_outputs(cfg, layout, geom, nwm_meta, extracted):
    """_summary_

    Args:
        cfg (_type_): _description_
        layout (_type_): _description_
        geom (_type_): _description_
        nwm_meta (_type_): _description_
        extracted (_type_): _description_

    Returns:
        _type_: _description_
    """
    if cfg.data_source == "forcings":
        return multiprocess_write_netcdf(
            cfg,
            layout.forcing_path,
            nwm_meta,
            extracted.data_array,
            geom.jcatchment_dict,
            extracted.t_ax,
        )
    if cfg.data_source == "channel_routing":
        if nwm_meta.fcst_cycle is None:
            filename = "qlaterals.nc"
        else:
            filename = (
                f"ngen.{nwm_meta.fcst_cycle}z.{nwm_meta.urlbase}.channel_routing."
                + f"{nwm_meta.lead_start}_{nwm_meta.lead_end}.nc"
            )
        return write_netcdf_chrt(
            cfg.storage_type,
            layout.forcing_path,
            extracted.data_array,
            extracted.t_ax,
            filename,
        )
    filename = f"channel_restart_{nwm_meta.restart_date}_{nwm_meta.restart_hour}0000.nc"
    return write_netcdf_restart(
        cfg.storage_type, layout.forcing_path, extracted.data_array, filename
    )


def write_outputs(cfg, layout, geom, nwm_meta, extracted, profiler):
    """
    Write the extracted data out in every requested file type.
    """
    written = WriteResult()
    with phase("FILEWRITING", profiler):
        if "netcdf" in cfg.output_file_type:
            written.netcdf_file_sizes_MB = write_netcdf_outputs(
                cfg, layout, geom, nwm_meta, extracted
            )
        if cfg.ii_verbose:
            print(
                f"Writing catchment forcings to {layout.output_path}!",
                end=None,
                flush=True,
            )
        if (
            cfg.ii_plot
            or cfg.ii_collect_stats
            or any(x in cfg.output_file_type for x in ["csv", "parquet", "tar"])
        ):
            if cfg.data_source == "forcings":
                catchments = list(geom.weights_df.index)
            elif cfg.data_source == "channel_routing":
                catchments = list(geom.nwm_ngen_map.keys())
            else:
                catchments = None

            if catchments is None:
                print("Dataframes don't get written for t-route restarts")
            else:
                (
                    written.forcing_cat_ids,
                    written.filenames,
                    written.cat_file_sizes_MB,
                    written.cat_file_sizes_zipped_MB,
                    written.tar_buffs,
                ) = multiprocess_write_df(
                    cfg,
                    extracted.data_array,
                    extracted.t_ax,
                    catchments,
                    layout.forcing_path,
                )

    if cfg.ii_verbose:
        write_time = profiler.timings["FILEWRITING"]
        print(
            f"\n\nWrite processs: {cfg.nprocs}\nWrite time: {write_time:.2f}"
            + f"\nWrite rate {geom.ncatchments / write_time:.2f} files/second\n",
            end=None,
            flush=True,
        )
    return written


def plot_outputs(cfg, layout, extracted, forcing_cat_ids):
    if cfg.gpkg_files[0].endswith(".parquet"):
        print("Plotting currently not implemented for parquet, need geopackage")
        return
    if len(cfg.gpkg_files) > 1:
        raise Warning(f"Plotting only the first geopackage {cfg.gpkg_files[0]}")

    cat_ids = ["cat-" + x for x in forcing_cat_ids]
    jplot_vars = np.array(
        [
            x
            for x in range(len(ngen_variables))
            if ngen_variables[x] in cfg.ngen_vars_plot
        ]
    )
    if cfg.storage_type == "s3":
        gif_out = Path("./GIFs")
    else:
        gif_out = Path(layout.meta_path, "GIFs")
    plot_ngen_forcings(
        extracted.nwm_data,
        extracted.data_array[:, jplot_vars, :],
        cfg.gpkg_files[0],
        extracted.t_ax,
        cat_ids,
        cfg.ngen_vars_plot,
        gif_out,
    )
    if cfg.storage_type == "s3":
        os.system(f"aws s3 sync ./GIFs {layout.meta_path}/GIFs")


def print_summary(cfg, layout, timings, runtime):
    print("\n\n--------SUMMARY-------")
    msg = f"\nData has been written to {layout.output_path}"
    if cfg.data_source == "forcings":
        msg += (
            f"\nCalc weights  : {timings['READWEIGHTS'] + timings['CALC_WINDOW']:.2f}s"
        )
    msg += f"\nProcess data  : {timings['PROCESSING']:.2f}s"
    msg += f"\nWrite data    : {timings['FILEWRITING']:.2f}s"
    if cfg.ii_collect_stats:
        msg += f"\nCollect stats : {timings['COLLECT_STATS']:.2f}s"
    if "tar" in cfg.output_file_type:
        msg += f"\nWrite tar     : {timings['TAR']:.2f}s"
    msg += f"\nRuntime       : {runtime:.2f}s\n"
    print(msg)


def fp_animation_and_filenames(cfg):
    msg = "\nForcingProcessor has awoken. Let's do this."
    for x in msg:
        print(x, end="")
        sys.stdout.flush()
        time.sleep(0.05)
    print("\n")
    print("NWM file names:")
    for jfile in cfg.nwm_forcing_files:
        print(f"{jfile}")


def tar_chunks(cfg, geom):
    if cfg.data_source == "channel_routing" and geom.nwm_ngen_map is not None:
        chunks = {1: list(geom.nwm_ngen_map.keys())}
    else:
        chunks = geom.jcatchment_dict

    return chunks


def prep_ngen_data(conf):
    """
    Primary function to retrieve forcing data and convert it into files that can be ingested into
    ngen.

    Inputs: forcingprocessor config file
        https://github.com/CIROH-UA/forcingprocessor/blob/main/configs/conf_fp.json

    Outputs: ngen forcing files of file type csv, parquet, netcdf, or gzippped tar

    Docs: https://github.com/CIROH-UA/forcingprocessor/blob/main/README.md
    """
    t_start = time.perf_counter()
    profiler = Profiler(log_file="./profile_fp.txt", timings={})

    profiler.log("FORCINGPROCESSOR_START")
    with phase("CONFIGURATION", profiler):
        cfg = read_config(conf)
        layout = build_output_layout(cfg)
        nwm_meta = parse_nwm_filenames(cfg)

    if cfg.ii_verbose:
        fp_animation_and_filenames(cfg)

    geom = load_geometry(cfg, profiler)

    with phase("STORE_INPUTS", profiler):
        s3_client = write_run_manifest(cfg, layout, geom.weights_df)

    extracted = extract(cfg, geom, nwm_meta, profiler)
    written = write_outputs(cfg, layout, geom, nwm_meta, extracted, profiler)
    core_runtime = time.perf_counter() - t_start

    if cfg.ii_plot:
        plot_outputs(cfg, layout, extracted, written.forcing_cat_ids)

    if cfg.ii_collect_stats:
        with phase("COLLECT_STATS", profiler):
            collect_metadata(
                cfg, layout, s3_client, geom, extracted, written, core_runtime
            )
    extracted.release()  # release data to manage memory

    if "tar" in cfg.output_file_type:
        with phase("TAR", profiler):
            if cfg.ii_verbose:
                print("\nWriting tarball...", flush=True)
            chunks = tar_chunks(cfg, geom)
            multiprocess_write_tar(
                cfg, layout.forcing_path, chunks, written.filenames, written.tar_buffs
            )

    if cfg.ii_verbose:
        total_runtime = sum(profiler.timings.values())
        print_summary(cfg, layout, profiler.timings, total_runtime)
    profiler.log("FORCINGPROCESSOR_END")

    if cfg.storage_type == "s3":
        bucket, key = convert_url2key(layout.metaf_path, cfg.storage_type)
        if s3_client is not None:
            s3_client.upload_file(profiler.log_file, bucket, key + "/profile_fp.txt")
        os.remove(profiler.log_file)
    else:
        shutil.move(profiler.log_file, Path(layout.metaf_path, "profile_fp.txt"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="infile",
        type=str,
        help="A json containing user inputs to run forcingprocessor",
    )
    args = parser.parse_args()

    if args.infile[0] == "{":
        conf = json.loads(args.infile)
    else:
        if "s3://" in args.infile:
            os.system(f"wget {args.infile}")
            filename = args.infile.split("/")[-1]
            conf = json.load(open(filename, encoding="utf-8"))
        else:
            conf = json.load(open(args.infile, encoding="utf-8"))

    prep_ngen_data(conf)


if __name__ == "__main__":
    main()
