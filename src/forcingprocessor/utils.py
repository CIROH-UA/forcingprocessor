"""Data processing and task distribution utility functions."""

import json
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd
import psutil
import s3fs
import xarray as xr

B2MB = 1048576

nwm_variables = [
    "U2D",
    "V2D",
    "LWDOWN",
    "RAINRATE",
    "RAINRATE",
    "T2D",
    "Q2D",
    "PSFC",
    "SWDOWN",
]

ngen_variables = [
    "UGRD_10maboveground",
    "VGRD_10maboveground",
    "DLWRF_surface",
    "APCP_surface",
    "precip_rate",
    "TMP_2maboveground",
    "SPFH_2maboveground",
    "PRES_surface",
    "DSWRF_surface",
]

vpus = [
    "01",
    "02",
    "03W",
    "03S",
    "03N",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10L",
    "10U",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
]


def get_window(weights_df: pd.DataFrame) -> tuple[int, int, int, int]:
    """
    Providing window on weights for which number of catchments is over 50,000

    Args:
        weights_df (pd.DataFrame): datastream weights df where the indicies are catchment ids and
            the columns are cell-id and coverage

    Returns:
        tuple[int, int, int, int]: x_min, x_max, y_min, y_max
    """
    nx = 4608
    ny = 3840
    if len(weights_df) < 50000:
        x_min_list = []
        x_max_list = []
        y_min_list = []
        y_max_list = []
        idx_2d = []
        for row in weights_df.itertuples():
            indices = row.cell_id
            idx_2d = np.unravel_index(indices, (1, nx, ny), order="F")  # type: ignore
            x_min_list.append(np.min(idx_2d[1]))
            x_max_list.append(np.max(idx_2d[1]))
            y_min_list.append(np.min(idx_2d[2]))
            y_max_list.append(np.max(idx_2d[2]))
        x_min = np.min(x_min_list)
        x_max = np.max(x_max_list)
        y_min = np.min(y_min_list)
        y_max = np.max(y_max_list)
    else:
        x_min = 0
        x_max = nx - 1
        y_min = 0
        y_max = ny - 1

    return x_min, x_max, y_min, y_max


@dataclass
class Profiler:
    """Log of the name of steps and their timings."""

    log_file: str
    timings: dict

    def log(self, label):
        """Write name of processing milestone and time of completion to log file.

        Args:
            label (str): Name of processing milestone.
        """
        timestamp = datetime.now(UTC).astimezone().strftime("%Y%m%d%H%M%S")
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"{label}: {timestamp}\n")


@contextmanager
def phase(label, profiler):
    """
    Bracket a step of the run with START/END entries in the profile log and
    record its duration in the timings dict.
    """
    profiler.log(f"{label}_START")
    t0 = time.perf_counter()
    yield

    profiler.timings[label] = time.perf_counter() - t0
    profiler.log(f"{label}_END")


def read_json(path: str | Path) -> dict:
    """Reads a json into a dict.

    Args:
        path (str | Path): Path to the JSON file

    Returns:
        dict: The contents of the JSON file
    """
    if "s3://" in str(path):
        with s3fs.S3FileSystem(anon=True).open(path, "r") as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_dataset(path: str | Path) -> xr.Dataset:
    """Reads a netCDF or zarr dataset into an xarray Dataset.

    Args:
        path (str | Path): Path to the netCDF or zarr file.

    Returns:
        xr.Dataset: The contents of the netCDF or zarr file
    """
    if "s3://" in str(path):
        with s3fs.S3FileSystem(anon=True).open(path, "rb") as f:
            return xr.open_dataset(f).load()
    return xr.open_dataset(path).load()


def distribute_work(items, nprocs):
    """
    Distribute items evenly between processes, round robin
    """
    items_per_proc = [0 for x in range(nprocs)]
    for j in range(len(items)):
        k = j % nprocs
        items_per_proc[k] = items_per_proc[k] + 1
    return items_per_proc


def load_balance(items_per_proc: list, ii_verbose: bool = False):
    """
    Drop the processes that were assigned no work.

    Args:
        items_per_proc (list): list of length number of processes with each element representing the
            number of items the process has been assigned
        ii_verbose (bool): If True, print information about the load balancing process. Defaults to
            False.
    """
    nprocs = len(items_per_proc)
    ntasked = len(np.nonzero(items_per_proc)[0])
    if nprocs > ntasked:
        if ii_verbose:
            print(
                f"Not enough work for {nprocs} requested processes, downsizing to {ntasked}"
            )
        items_per_proc = items_per_proc[:ntasked]
    if ii_verbose:
        print(f"item distribution {items_per_proc}")
    return items_per_proc


def report_usage() -> tuple[float, float, float]:
    """Prints out summary of resource usage.

    Returns:
        tuple[float, float, float]: RAM usage in GB, RAM usage in percent, CPU usage in percent
    """
    usage_ram = psutil.virtual_memory()[3] / 1000000000
    percent_ram = psutil.virtual_memory()[2]
    percent_cpu = psutil.cpu_percent()
    print(
        f"\nCurrent RAM usage (GB): {usage_ram:.2f}, {percent_ram:.2f}%"
        + f"\nCurrent CPU usage : {percent_cpu:.2f}%"
    )
    return usage_ram, percent_ram, percent_cpu


def convert_url2key(
    nwm_file: str | Path, fs_type: str | None
) -> tuple[str | None, str]:
    """Convert a NWM file URL to a bucket and key for cloud storage.

    Args:
        nwm_file (str | Path): Name of NWM file
        fs_type (str | None): File system type, either "google" or "s3" or None.

    Returns:
        tuple[str | None, str]: The bucket name and the key for the file in cloud storage.
    """
    bucket_key = ""
    if isinstance(nwm_file, Path):
        nwm_file = str(nwm_file)
    _nc_file_parts = nwm_file.split("/")
    layers = _nc_file_parts[3:]
    for jlay in layers:
        if jlay == layers[-1]:
            bucket_key += jlay
        else:
            bucket_key += jlay + "/"
    if fs_type == "google":
        bucket = _nc_file_parts[3]
    elif fs_type == "s3":
        bucket = _nc_file_parts[2]
    else:
        bucket = None

    return bucket, bucket_key


def make_forcing_netcdf(
    out_path: str | Path,
    catchments: np.ndarray,
    t_ax: np.ndarray,
    input_array: np.ndarray,
) -> None:
    """
    Create a netcdf file with the forcing data.

    Parameters:
        out_path (str | Path): Path to save the netcdf file.
        catchments (np.ndarray): Array of catchment IDs.
        t_ax (np.ndarray): Time axis array with shape (nt,).
        input_array (np.ndarray): Forcing data array with shape (ncat, nt, forcing variables).
    """

    with nc.Dataset(out_path, "w", format="NETCDF4") as ds:  # pylint: disable=no-member
        ds.createDimension("catchment-id", len(catchments))
        ds.createDimension("time", len(t_ax))

        ids_var = ds.createVariable("ids", str, ("catchment-id",))
        ids_var[:] = catchments

        time_var = ds.createVariable("Time", "f8", ("catchment-id", "time"))
        time_var[:] = t_ax

        for i, var_name in enumerate(ngen_variables):
            var = ds.createVariable(var_name, "f8", ("catchment-id", "time"))
            var[:] = input_array[:, :, i]


def normalize_vpu_id(value: str) -> str:
    """
    Normalize a VPU identifier to the standard VPU_XX format.

    Examples:
        03W -> VPU_03W
        vpu_03w -> VPU_03W
        nextgen_VPU_03W.gpkg -> VPU_03W

    Args:
        value (str): The VPU identifier to normalize.

    Returns:
        str: The normalized VPU identifier in the format VPU_XX.
    """
    name = Path(str(value)).stem

    match = re.search(r"(?i)vpu[-_]?(\d{2}[A-Z]?)", name)
    if match:
        return f"VPU_{match.group(1).upper()}"

    if re.fullmatch(r"\d{2}[A-Za-z]?", name):
        return f"VPU_{name.upper()}"

    return name
