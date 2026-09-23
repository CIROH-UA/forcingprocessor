"""Channel routing: q_lateral per NGEN nexus, for t-route.

The mapping kernel lives in channel_routing_tools.py. What is here is the plumbing that feeds it.
"""

import concurrent.futures as cf
import os
import time
from dataclasses import dataclass

import gcsfs
import geopandas as gpd
import numpy as np
import pandas as pd
import s3fs
import xarray as xr

from forcingprocessor.channel_routing_tools import (
    mapped_nwm_ids,
    read_qlateral,
    sum_to_nexus,
    write_netcdf_chrt,
)
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
    open_nwm_file,
    phase,
    pool_filesystem,
    read_json,
    report_usage,
)

# s3://noaa-nwm-pds/nwm.20241029/analysis_assim/nwm.t16z.analysis_assim.channel_rt.tm00.conus.nc
FILENAME_PATTERN = r"nwm\.(\d{8})/(\w+)/nwm\.(\w+)(\d{2})z\.\w+\.channel_rt[^\.]*\.(\w+)(\d{2})\.conus\.nc"

# Nexus ids the hydrofabric uses for terminal, coastal and internal nexuses.
SYNTHETIC_NEXUS_PREFIXES = ["tnx", "cnx", "inx"]


@dataclass(kw_only=True)
class ChannelRoutingGeometry(Geometry):
    """Which NWM feature ids drain to each NGEN nexus. Required, so nothing checks it for None."""

    nwm_ngen_map: dict


@dataclass
class ExtractJob:
    """Everything one extraction worker needs. Must be picklable."""

    files: list[str]
    mapping: dict
    fs: s3fs.S3FileSystem | str | None
    fs_type: str | None
    ii_verbose: bool = False


def parse_filenames(files: list[str]) -> NWMFileMetadata:
    """Extract forecast cycle and lead time from the CHRTOUT file names."""
    return parse_cycle_and_lead(files, FILENAME_PATTERN)


def load_geometry(cfg: RunConfig, profiler: Profiler) -> ChannelRoutingGeometry:
    """Read the nexus to NWM id map, restricted to the nexuses in this geopackage."""
    with phase("READMAP", profiler):
        if cfg.ii_verbose:
            print("Reading NWM to NGEN map\n", flush=True)
        full_nwm_ngen_map = read_json(cfg.map_file)
        catchments = gpd.read_file(cfg.gpkg_files[0], layer="nexus")["id"].to_list()
        nwm_ngen_map = {
            jcatch: full_nwm_ngen_map[jcatch]
            for jcatch in catchments
            if not any(x in jcatch for x in SYNTHETIC_NEXUS_PREFIXES)
        }
    return ChannelRoutingGeometry(
        ncatchments=len(nwm_ngen_map), nwm_ngen_map=nwm_ngen_map
    )


def _extract_chunk(job: ExtractJob) -> list:
    """Map one worker's share of the CHRTOUT files onto nexuses.

    A list is returned instead of a tuple for multiprocessing purposes.
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
    nwm_cats = mapped_nwm_ids(job.mapping)
    if job.ii_verbose:
        print(
            f"Process #{pid} extracting data from {nfiles} files", end=None, flush=True
        )

    data_list = []
    t_list = []
    nwm_file_sizes_MB = []

    for j, nwm_file in enumerate(job.files):
        t0 = time.perf_counter()
        file_obj, size_MB = open_nwm_file(nwm_file, fs, job.fs_type)
        nwm_file_sizes_MB.append(size_MB)
        topen += time.perf_counter() - t0

        t0 = time.perf_counter()
        with xr.open_dataset(file_obj, chunks={}) as nwm_data:
            data_allnwm, t, valid_nwm_set = read_qlateral(nwm_data, nwm_file, nwm_cats)
        tread += time.perf_counter() - t0
        t_list.append(t)

        t0 = time.perf_counter()
        data_list.append(sum_to_nexus(data_allnwm, job.mapping, valid_nwm_set))
        tdata += time.perf_counter() - t0

        if job.ii_verbose:
            ttotal = topen + tread + tdata
            print(
                f"\nAverage time for:\nfs open file: {topen / (j + 1):.2f} s"
                + f"\nread q_lateral: {tread / (j + 1):.2f} s"
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
    return [data_list, t_list, nwm_file_sizes_MB]


def extract(cfg: RunConfig, geom: ChannelRoutingGeometry) -> Extracted:
    """Map every CHRTOUT file onto nexuses, spread over a process pool."""
    files_per_proc = distribute_work(cfg.nwm_forcing_files, cfg.nprocs)
    files_per_proc = load_balance(files_per_proc, cfg.ii_verbose)

    jobs = []
    start = 0
    for count in files_per_proc:
        end = min(start + count, len(cfg.nwm_forcing_files))
        jobs.append(
            ExtractJob(
                files=cfg.nwm_forcing_files[start:end],
                mapping=geom.nwm_ngen_map,
                fs=pool_filesystem(cfg.fs_type),
                fs_type=cfg.fs_type,
                ii_verbose=cfg.ii_verbose,
            )
        )
        start = end

    with cf.ProcessPoolExecutor(max_workers=len(jobs)) as pool:
        results = list(pool.map(_extract_chunk, jobs))
    print("Processes have returned")

    # Nexus ids and q_lateral share an axis, so the array stays object typed.
    data_array = np.concatenate([r[0] for r in results]).astype(object)
    data_array[:, :, 1] = data_array[:, :, 1].astype(float)

    return Extracted(
        data_array=data_array,
        t_ax=[t for r in results for t in r[1]],
        nwm_file_sizes_MB=[s for r in results for s in r[2]],
    )


def write_netcdf(
    cfg: RunConfig,
    layout: OutputLayout,
    geom: ChannelRoutingGeometry,
    nwm_meta: NWMFileMetadata,
    extracted: Extracted,
) -> list[float]:
    """Write one netcdf of q_laterals for the whole run."""
    if nwm_meta.fcst_cycle is None:
        filename = "qlaterals.nc"
    else:
        filename = (
            f"ngen.{nwm_meta.fcst_cycle}z.{nwm_meta.urlbase}.channel_routing."
            + f"{nwm_meta.lead_start}_{nwm_meta.lead_end}.nc"
        )
    if not isinstance(extracted.data_array, np.ndarray) or not isinstance(
        extracted.t_ax, list
    ):
        raise TypeError(
            "extracted.data_array must be an np.ndarray and extracted.t_ax must be a list"
        )
    return write_netcdf_chrt(
        cfg.storage_type,
        layout.forcing_path,
        extracted.data_array,
        extracted.t_ax,
        filename,
    )


def catchment_ids(geom: ChannelRoutingGeometry) -> list:
    """Nexus ids in the order the extracted array holds them."""
    return list(geom.nwm_ngen_map.keys())


def tar_chunks(geom: ChannelRoutingGeometry) -> dict:
    """A channel routing run writes a single tarball."""
    return {1: catchment_ids(geom)}


def slice_catchments(data: np.ndarray, start: int, end: int) -> np.ndarray:
    """Nexuses are the middle axis of the channel routing array."""
    return data[:, start:end, :]


def build_frame(
    data: np.ndarray, t_ax: list, j: int, catchment_id: str
) -> tuple[pd.DataFrame, str, None]:
    """One nexus' q_lateral series in the column order t-route expects.

    No reported id is returned because channel routing runs do not populate forcing_cat_ids.
    """
    df = pd.DataFrame(data[:, j, :], columns=["feature_id", "q_lateral"])
    df = df[["q_lateral"]]
    df["time"] = t_ax
    return df[["time", "q_lateral"]], catchment_id, None


def summarize(
    cfg: RunConfig, extracted: Extracted, written: WriteResult, runtime: float
) -> dict:
    """Input and output file sizes for this run."""
    return size_summary(cfg, extracted, written, runtime)


def stat_frames(
    cfg: RunConfig,
    geom: ChannelRoutingGeometry,
    extracted: Extracted,
    written: WriteResult,
) -> list[tuple[pd.DataFrame, str]]:
    """Per nexus averages and medians of q_lateral over the time axis."""
    if not isinstance(extracted.data_array, np.ndarray):
        return []
    ids = catchment_ids(geom)
    qlat = extracted.data_array[:, :, 1]

    avg_df = pd.DataFrame(np.average(qlat, axis=0).T, columns=["q_lateral"])
    avg_df.insert(0, "nexus id", ids)
    med_df = pd.DataFrame(np.median(qlat, axis=0).T, columns=["q_lateral"])
    med_df.insert(0, "nexus id", ids)
    return [(avg_df, "catchments_avg.csv"), (med_df, "catchments_median.csv")]


MODE = Mode(
    name="channel_routing",
    forcing_subdir=("outputs", "ngen"),
    supports_plotting=False,
    # t-route reads these headerless, with the index column present.
    csv_options={"header": False},
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
