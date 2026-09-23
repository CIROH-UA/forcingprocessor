"""t-route restarts: channel depth and streamflow at a single analysis time.

The depth solve lives in troute_restart_tools.py. A restart has no time axis and writes one file, so
the per catchment frame, tar and plotting steps do not apply to it.
"""

import re
from dataclasses import dataclass

import numpy as np
import pandas as pd
import xarray as xr

from forcingprocessor.metadata import summarize_sizes
from forcingprocessor.modes import Mode
from forcingprocessor.records import (
    Extracted,
    Geometry,
    NWMFileMetadata,
    OutputLayout,
    RunConfig,
    WriteResult,
)
from forcingprocessor.troute_restart_tools import create_restart, write_netcdf_restart
from forcingprocessor.utils import (
    Profiler,
    open_nwm_file,
    pool_filesystem,
    read_dataset,
    read_json,
)

# s3://noaa-nwm-pds/nwm.20241029/analysis_assim/nwm.t16z.analysis_assim.channel_rt.tm00.conus.nc
FILENAME_PATTERN = r"nwm\.(\d{8})/analysis_assim/nwm\.t(\d{2})z\.analysis_assim\.channel_rt\.tm00\.conus\.nc"


@dataclass(kw_only=True)
class RestartGeometry(Geometry):
    """The NGEN to NWM catchment map and the channel geometry it is solved against.

    All three are required, so nothing downstream has to check them for None.
    """

    cat_map: dict
    crosswalk_ds: xr.Dataset
    routelink_ds: xr.Dataset


def parse_filenames(files: list[str]) -> NWMFileMetadata:
    """Extract the restart date and hour from the single analysis file name.

    Args:
        files (list[str]): The NWM files this run reads.

    Returns:
        NWMFileMetadata: Information about the NWM data sourced from the URL.
    """
    meta = NWMFileMetadata()
    match = re.search(FILENAME_PATTERN, files[0])
    if match:
        meta.restart_date = match.group(1)
        meta.restart_hour = match.group(2)
    else:
        print("Could not extract restart date and time")
    return meta


def load_geometry(cfg: RunConfig, profiler: Profiler) -> RestartGeometry:
    """Read the catchment map, crosswalk, and RouteLink channel geometry."""
    return RestartGeometry(
        ncatchments=1,
        cat_map=read_json(cfg.restart_map_file),
        crosswalk_ds=read_dataset(cfg.crosswalk_file),
        routelink_ds=read_dataset(cfg.routelink_file),
    )


def extract(cfg: RunConfig, geom: RestartGeometry) -> Extracted:
    """Solve depth and streamflow from the single analysis assimilation file."""
    fs = pool_filesystem(cfg.fs_type)
    if fs == "google":
        import gcsfs  # pylint: disable=import-outside-toplevel

        fs = gcsfs.GCSFileSystem()

    file_obj, size_MB = open_nwm_file(cfg.nwm_forcing_files[0], fs, cfg.fs_type)
    nwm_ds = xr.open_dataset(file_obj).load()

    if geom.cat_map is None or geom.crosswalk_ds is None or geom.routelink_ds is None:
        raise TypeError(
            "geom.cat_map, geom.crosswalk_ds, and geom.routelink_ds must not be None"
        )

    return Extracted(
        data_array=create_restart(
            geom.cat_map, geom.crosswalk_ds, nwm_ds, geom.routelink_ds
        ),
        nwm_file_sizes_MB=[size_MB],
    )


def write_netcdf(
    cfg: RunConfig,
    layout: OutputLayout,
    geom: RestartGeometry,
    nwm_meta: NWMFileMetadata,
    extracted: Extracted,
) -> list[float]:
    """Write the t-route restart netcdf."""
    filename = f"channel_restart_{nwm_meta.restart_date}_{nwm_meta.restart_hour}0000.nc"
    if not isinstance(extracted.data_array, xr.Dataset):
        raise TypeError("extracted.data_array must be an xr.Dataset")
    return write_netcdf_restart(
        cfg.storage_type, layout.forcing_path, extracted.data_array, filename
    )


def catchment_ids(geom: RestartGeometry) -> None:
    """A restart is a single file, so there are no per catchment frames to write."""
    return None


def tar_chunks(geom: RestartGeometry) -> None:
    """TAR file writing is not implemented for t-route restarts."""
    return None


def slice_catchments(data: np.ndarray, start: int, end: int) -> np.ndarray:
    """Unused: t-route restarts write no per catchment frames."""
    raise NotImplementedError("t-route restarts do not write per catchment frames")


def build_frame(
    data: np.ndarray, t_ax: list, j: int, catchment_id: str
) -> tuple[pd.DataFrame, str, None]:
    """Unused: t-route restarts write no per catchment frames."""
    raise NotImplementedError("t-route restarts do not write per catchment frames")


def summarize(
    cfg: RunConfig, extracted: Extracted, written: WriteResult, runtime: float
) -> dict:
    """Input and output file sizes for this run."""
    nwm_avg, _, _ = summarize_sizes(extracted.nwm_file_sizes_MB)
    nc_avg, _, _ = summarize_sizes(written.netcdf_file_sizes_MB)
    return {
        "runtime_s": [round(runtime, 2)],
        "nwmfiles_input": [len(cfg.nwm_forcing_files)],
        "nwm_file_size": [nwm_avg],
        "netcdf_catch_file_size_MB": [nc_avg],
    }


def stat_frames(
    cfg: RunConfig,
    geom: RestartGeometry,
    extracted: Extracted,
    written: WriteResult,
) -> list[tuple[pd.DataFrame, str]]:
    """A restart has no time axis, so there is nothing to average over."""
    return []


MODE = Mode(
    name="troute_restarts",
    forcing_subdir=("restart",),
    supports_plotting=False,
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
