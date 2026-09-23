"""Catchment forcings: area weighted NWM grid values, one record per catchment.

The science lives in averaging.py and weights.py. What is here is the plumbing that feeds them:
reading a window out of a netcdf, spreading files over processes, and naming the outputs.
"""

import concurrent.futures as cf
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import gcsfs
import numpy as np
import pandas as pd
import s3fs
import xarray as xr

from forcingprocessor.averaging import average_to_catchments, prepare_weights
from forcingprocessor.metadata import size_summary
from forcingprocessor.modes import Mode, parse_cycle_and_lead
from forcingprocessor.records import (
    Extracted,
    Geometry,
    NWMFileMetadata,
    OutputLayout,
    RunConfig,
    WriteResult,
)
from forcingprocessor.utils import (
    Profiler,
    distribute_work,
    load_balance,
    ngen_variables,
    nwm_variables,
    open_nwm_file,
    phase,
    pool_filesystem,
    report_usage,
)
from forcingprocessor.weights import Window, grid_window
from forcingprocessor.weights_hf2ds import multiprocess_hf2ds
from forcingprocessor.writers import multiprocess_write_netcdf

# s3://noaa-nwm-pds/nwm.20241029/forcing_short_range/nwm.t00z.short_range.forcing.f001.conus.nc
FILENAME_PATTERN = r"nwm\.(\d{8})/forcing_(\w+)/nwm\.(\w+)(\d{2})z\.\w+\.forcing\.(\w+)(\d{2})\.conus\.nc"


@dataclass(kw_only=True)
class ForcingGeometry(Geometry):
    """The weight table, the grid window it falls in, and the VPU grouping.

    All three are required, so nothing downstream has to check them for None.
    """

    weights_df: pd.DataFrame
    jcatchment_dict: dict
    window: Window


@dataclass
class ExtractJob:
    """Everything one extraction worker needs. Must be picklable."""

    files: list[str]
    weights_df: pd.DataFrame
    window: Window
    fs: s3fs.S3FileSystem | str | None
    fs_type: str | None
    ii_verbose: bool = False
    ii_plot: bool = False
    nts_plot: int = 0
    plot_vars: np.ndarray = field(default_factory=lambda: np.array([], dtype=int))


def parse_filenames(files: list[str]) -> NWMFileMetadata:
    """Extract forecast cycle and lead time from the NWM forcing file names."""
    return parse_cycle_and_lead(files, FILENAME_PATTERN)


def load_geometry(cfg: RunConfig, profiler: Profiler) -> ForcingGeometry:
    """Read the weights for every catchment and the grid window they fall inside."""
    with phase("READWEIGHTS", profiler):
        if cfg.ii_verbose:
            print("Obtaining weights\n", flush=True)
        weights_df, jcatchment_dict = multiprocess_hf2ds(
            cfg.gpkg_files, cfg.nwm_forcing_files[0], cfg.nprocs
        )
    with phase("CALC_WINDOW", profiler):
        window = grid_window(weights_df)

    return ForcingGeometry(
        ncatchments=len(weights_df),
        weights_df=weights_df,
        jcatchment_dict=jcatchment_dict,
        window=window,
    )


def _read_window(
    nwm_data: xr.Dataset, nwm_file: str, window: Window
) -> tuple[np.ndarray, str, tuple[int, int]]:
    """Slice every NWM variable to the window and flip it north up.

    Retrospective files name their dimensions south_north/west_east and date stamp the filename;
    operational files use x/y and carry the valid time in an attribute.

    Args:
        nwm_data (xr.Dataset): An open NWM forcing file.
        nwm_file (str): The file's name, which carries the timestamp for retrospective data.
        window (Window): The grid subset to read.

    Returns:
        tuple[np.ndarray, str, tuple[int, int]]: Values of shape (nvar, window.ny, window.nx), the
            model output valid time, and the full grid shape the file was written on.
    """
    shp = nwm_data["U2D"].shape
    ny, nx = shp[1], shp[2]

    retro = "retrospective-2-1" in nwm_file or (
        "south_north" in nwm_data.dims and "west_east" in nwm_data.dims
    )
    if retro:
        x_dim, y_dim = "west_east", "south_north"
        t = datetime.strftime(
            datetime.strptime(
                nwm_file.split("/")[-1].split(".")[0], "%Y%m%d%H"
            ).replace(tzinfo=UTC),
            "%Y-%m-%d %H:%M:%S",
        )
    else:
        x_dim, y_dim = "x", "y"
        time_splt = nwm_data.attrs["model_output_valid_time"].split("_")
        t = time_splt[0] + " " + time_splt[1]

    selection = {
        x_dim: slice(window.x_min, window.x_max + 1),
        y_dim: slice(ny - (window.y_max + 1), ny - window.y_min),
    }
    values = np.zeros(
        shape=(len(nwm_variables), window.ny, window.nx), dtype=np.float64
    )
    for var_dx, jvar in enumerate(nwm_variables):
        values[var_dx, :, :] = np.flip(
            np.squeeze(nwm_data[jvar].isel(**selection).values), axis=0
        )

    return values, t, (nx, ny)


def _extract_chunk(job: ExtractJob) -> list:
    """Average one worker's share of the NWM files.

    Reads a file, averages it, then discards the grid, so a run never holds a stack of grids in
    memory. A list is returned instead of a tuple for multiprocessing purposes.
    """
    if job.fs_type == "google":
        fs = gcsfs.GCSFileSystem()
    else:
        fs = job.fs

    topen = 0
    tread = 0
    tdata = 0
    pid = os.getpid()
    nfiles = len(job.files)
    if job.ii_verbose:
        print(
            f"Process #{pid} extracting data from {nfiles} files", end=None, flush=True
        )

    prepared = None
    data_list = []
    t_list = []
    nwm_data_plot = []
    nwm_file_sizes_MB = []

    for j, nwm_file in enumerate(job.files):
        t0 = time.perf_counter()
        file_obj, size_MB = open_nwm_file(nwm_file, fs, job.fs_type)
        nwm_file_sizes_MB.append(size_MB)
        topen += time.perf_counter() - t0

        t0 = time.perf_counter()
        with xr.open_dataset(file_obj) as nwm_data:
            values, t, (grid_nx, grid_ny) = _read_window(nwm_data, nwm_file, job.window)
        tread += time.perf_counter() - t0

        if prepared is None:
            prepared = prepare_weights(job.weights_df, job.window, grid_nx, grid_ny)

        t_list.append(t)
        if job.ii_plot and j < job.nts_plot:
            nwm_data_plot.append(values[job.plot_vars, :, :])

        t0 = time.perf_counter()
        data_list.append(average_to_catchments(values, prepared))
        del values
        tdata += time.perf_counter() - t0

        if job.ii_verbose:
            ttotal = topen + tread + tdata
            print(
                f"\nAverage time for:\nfs open file: {topen / (j + 1):.2f} s"
                + f"\nread window: {tread / (j + 1):.2f} s"
                + f"\ncalculate catchment values: {tdata / (j + 1):.2f} s"
                + f"\ntotal {ttotal / (j + 1):.2f} s"
                + f"\npercent complete {100 * (j + 1) / nfiles:.2f}",
                end=None,
                flush=True,
            )
        report_usage()

    if job.ii_verbose:
        print(
            f"Process #{pid} completed data extraction, returning data to primary process",
            flush=True,
        )
    return [data_list, t_list, nwm_data_plot, nwm_file_sizes_MB]


def extract(cfg: RunConfig, geom: ForcingGeometry) -> Extracted:
    """Average every NWM file to catchments, spread over a process pool."""
    files_per_proc = distribute_work(cfg.nwm_forcing_files, cfg.nprocs)
    files_per_proc = load_balance(files_per_proc, cfg.ii_verbose)
    plot_vars = np.array(
        [
            x
            for x in range(len(ngen_variables))
            if ngen_variables[x] in cfg.ngen_vars_plot
        ]
    )

    jobs = []
    start = 0
    for count in files_per_proc:
        end = min(start + count, len(cfg.nwm_forcing_files))
        jobs.append(
            ExtractJob(
                files=cfg.nwm_forcing_files[start:end],
                weights_df=geom.weights_df,
                window=geom.window,
                fs=pool_filesystem(cfg.fs_type),
                fs_type=cfg.fs_type,
                ii_verbose=cfg.ii_verbose,
                ii_plot=cfg.ii_plot,
                nts_plot=cfg.nts_plot,
                plot_vars=plot_vars,
            )
        )
        start = end

    with cf.ProcessPoolExecutor(max_workers=len(jobs)) as pool:
        results = list(pool.map(_extract_chunk, jobs))
    print("Processes have returned")

    return Extracted(
        data_array=np.concatenate([r[0] for r in results]),
        t_ax=[t for r in results for t in r[1]],
        nwm_data=np.concatenate([r[2] for r in results]),
        nwm_file_sizes_MB=[s for r in results for s in r[3]],
    )


def write_netcdf(
    cfg: RunConfig,
    layout: OutputLayout,
    geom: ForcingGeometry,
    nwm_meta: NWMFileMetadata,
    extracted: Extracted,
) -> list[float]:
    """Write one netcdf of catchment forcings per VPU."""
    return multiprocess_write_netcdf(
        cfg,
        layout.forcing_path,
        nwm_meta,
        extracted.data_array,
        geom.jcatchment_dict,
        extracted.t_ax,
    )


def catchment_ids(geom: ForcingGeometry) -> list:
    """Catchment ids in the order the extracted array holds them."""
    return list(geom.weights_df.index)


def tar_chunks(geom: ForcingGeometry) -> dict | None:
    """One tarball per VPU."""
    return geom.jcatchment_dict


def slice_catchments(data: np.ndarray, start: int, end: int) -> np.ndarray:
    """Catchments are the last axis of the forcings array."""
    return data[:, :, start:end]


def build_frame(
    data: np.ndarray, t_ax: list, j: int, catchment_id: str
) -> tuple[pd.DataFrame, str, str]:
    """One catchment's time series, its filename stem, and the id metadata reports."""
    df = pd.DataFrame(data[:, :, j], columns=ngen_variables)
    df.insert(0, "time", t_ax)
    cat_id = catchment_id.split("-")[1]
    return df, f"cat-{cat_id}", cat_id


def summarize(
    cfg: RunConfig, extracted: Extracted, written: WriteResult, runtime: float
) -> dict:
    """Input and output file sizes for this run."""
    return size_summary(
        cfg, extracted, written, runtime, len(nwm_variables), len(ngen_variables)
    )


def _calculate_vpu_precip_stats(
    data_array: np.ndarray, catchment_ids_in_order: list, jcatchment_dict: dict
) -> pd.DataFrame:
    """
    Calculate compact precipitation statistics for each VPU.

    Parameters
    ----------
    data_array : np.ndarray
        Forcing data with dimensions (time, variable, catchment).
    catchment_ids_in_order : list
        Catchment IDs corresponding to the catchment axis of data_array.
    jcatchment_dict : dict
        Mapping of VPU IDs to catchment IDs.

    Returns
    -------
    pd.DataFrame
        One row per VPU containing precipitation summary statistics.
    """
    precip_idx = ngen_variables.index("precip_rate")
    catchment_index = {
        str(catchment_id): i for i, catchment_id in enumerate(catchment_ids_in_order)
    }

    rows = []

    for vpu_id, vpu_catchments in jcatchment_dict.items():
        indices = [
            catchment_index[str(catchment_id)]
            for catchment_id in vpu_catchments
            if str(catchment_id) in catchment_index
        ]

        if not indices:
            continue

        precip = data_array[:, precip_idx, indices]

        rows.append(
            {
                "vpu_id": vpu_id,
                "precip_min": float(np.min(precip)),
                "precip_max": float(np.max(precip)),
                "precip_mean": float(np.mean(precip)),
                "precip_sum": float(np.sum(precip)),
                "precip_nonzero_fraction": float(
                    np.count_nonzero(precip) / precip.size
                ),
            }
        )

    return pd.DataFrame(rows)


def stat_frames(
    cfg: RunConfig,
    geom: ForcingGeometry,
    extracted: Extracted,
    written: WriteResult,
) -> list[tuple[pd.DataFrame, str]]:
    """Per VPU precipitation statistics, plus per catchment averages and medians."""
    data = extracted.data_array
    ids = written.forcing_cat_ids
    if not isinstance(data, np.ndarray) or ids is None:
        return []

    # Issue 9: write compact VPU-level precipitation statistics.
    frames = [
        (
            _calculate_vpu_precip_stats(
                data, list(geom.weights_df.index), geom.jcatchment_dict
            ),
            "metadata_by_vpu.csv",
        )
    ]

    avg_df = pd.DataFrame(np.average(data, axis=0).T, columns=ngen_variables)
    avg_df.insert(0, "catchment id", ids)
    med_df = pd.DataFrame(np.median(data, axis=0).T, columns=ngen_variables)
    med_df.insert(0, "catchment id", ids)
    frames += [(avg_df, "catchments_avg.csv"), (med_df, "catchments_median.csv")]
    return frames


MODE = Mode(
    name="forcings",
    forcing_subdir=("forcings",),
    supports_plotting=True,
    csv_options={"index": False},
    parse_filenames=parse_filenames,
    load_geometry=load_geometry,
    extract=extract,
    write_netcdf=write_netcdf,
    catchment_ids=catchment_ids,
    tar_chunks=tar_chunks,
    slice_catchments=slice_catchments,
    build_frame=build_frame,
    summarize=summarize,
    stat_frames=stat_frames,
)
