"""The data shapes passed between the workflow steps.

These live in one module, importable from anywhere in the package, so that config.py, metadata.py,
writers.py and the mode implementations can all be typed against them without importing the
workflow that produces them.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


@dataclass(frozen=True)
class Mode:
    """What varies between the three run types: forcings, channel routing and t-route restarts.

    read_config binds one Mode for the run and every layer below receives the already bound
    implementation, so no step has to re-decide what kind of run it is in. Every field is a module
    level function or a plain value so that a Mode survives pickling into a worker process.
    """

    name: str
    forcing_subdir: tuple[str, ...]
    supports_plotting: bool
    csv_options: dict
    parse_filenames: Callable
    """(files: list[str]) -> NWMFileMetadata"""
    load_geometry: Callable
    """(cfg: RunConfig, profiler: Profiler) -> Geometry"""
    extract: Callable
    """(cfg: RunConfig, geom: Geometry) -> Extracted"""
    write_netcdf: Callable
    """(cfg, layout, geom, nwm_meta, extracted) -> list[float]"""
    catchment_ids: Callable
    """(geom: Geometry) -> list | None, None when the mode writes no per catchment frames"""
    tar_chunks: Callable
    """(geom: Geometry) -> dict | None"""
    slice_catchments: Callable
    """(data: np.ndarray, start: int, end: int) -> np.ndarray"""
    build_frame: Callable
    """(data, t_ax, j, catchment_id) -> (pd.DataFrame, filename stem, reported id | None)"""
    summarize: Callable
    """(cfg, extracted, written, runtime) -> dict for metadata.csv"""
    stat_frames: Callable
    """(cfg, geom, extracted, written) -> list[tuple[pd.DataFrame, str]]"""


@dataclass
class RunConfig:
    """Contains information from the forcingprocessor configuration file.

    The run's Mode is bound here, once, by read_config.
    """

    conf: dict
    mode: Mode
    gpkg_files: list
    vpu_ids: list
    nwm_file: str
    nwm_forcing_files: list
    map_file: str
    restart_map_file: str
    crosswalk_file: str
    routelink_file: str
    output_path: str
    output_file_type: list
    storage_type: str
    fs_type: str | None
    nprocs: int
    ii_verbose: bool
    ii_collect_stats: bool
    ii_plot: bool
    nts_plot: int
    ngen_vars_plot: list


@dataclass
class OutputLayout:
    """Contains information on the path(s) where output files should be written."""

    output_path: Path | str
    forcing_path: Path | str
    meta_path: Path | str
    metaf_path: Path | str


@dataclass
class NWMFileMetadata:
    """Contains information about the NWM data derived from the NWM file source URL."""

    urlbase: str = ""
    fcst_cycle: str | None = None
    lead_start: str = ""
    lead_end: str = ""
    restart_date: str = ""
    restart_hour: str = ""


@dataclass(kw_only=True)
class Geometry:
    """What every run type's geometry reports.

    Each mode subclasses this with the fields its own extraction needs, declaring them required, so
    a consumer never has to work out which half of a union is live or guard against None.

    weights_df is optional here because only forcings runs have a weight table to archive; every
    other field a mode needs lives on that mode's subclass.
    """

    ncatchments: int = 0
    weights_df: pd.DataFrame | None = None


@dataclass
class Extracted:
    """Data pulled out of the NWM files, ordered in time."""

    data_array: xr.Dataset | np.ndarray | None
    t_ax: list | None = None
    nwm_data: np.ndarray | None = None
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
