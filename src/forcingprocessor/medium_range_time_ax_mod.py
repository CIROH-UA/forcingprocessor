"""
Author: Jordan Laser, Lynker

This script reads in a NRDS forcing file and shifts the time coordinate for medium range ensemble
members
https://github.com/CIROH-UA/ngen-datastream/issues/202
"""

import re
from pathlib import Path

import numpy as np
import xarray as xr

from forcingprocessor.utils import make_forcing_netcdf


def cut_forcing_data_for_ensemble(
    ds: xr.Dataset, ens_member: int, time_shift_hours: int = 6
) -> xr.Dataset:
    """
    Shift the time axis of the dataset based on the ensemble member.

    https://onlinelibrary.wiley.com/doi/epdf/10.1111/1752-1688.13184

    Args:
        ds (xarray.Dataset): Input dataset with Time coordinate.
        ens_member (int): Ensemble member number (2-7).
        time_shift_hours (int): Number of hours to shift the time coordinate.

    Returns:
        xr.Dataset: Shifted time axis.
    """
    start_cut = (ens_member - 1) * time_shift_hours
    end_cut = 204 + start_cut
    print(
        f"Member {ens_member}, cutting data from time axis index {start_cut} to {end_cut}"
    )
    out_ds = ds.isel(time=slice(start_cut, end_cut))
    return out_ds


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Shift time coordinate for medium range ensemble members"
    )
    parser.add_argument(
        "--input_file_ens0",
        type=str,
        required=True,
        help="Input NRDS forcing file. Must be from first ensemble member (ens0)",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=False,
        default=".",
        help="Output NRDS forcing file with shifted time coordinate",
    )
    parser.add_argument(
        "--ensemble_member",
        type=str,
        required=True,
        help="Ensember member (2-7)",
    )

    TIME_SHIFT_HOURS = 6

    args = parser.parse_args()
    assert Path(args.output_dir).is_dir(), "Output directory does not exist"

    # Cut dataset based on ensemble member
    ds_in = xr.open_dataset(args.input_file_ens0)
    ensemble_member = int(args.ensemble_member)
    if ensemble_member > 1 and ensemble_member < 8:
        ds_mod = cut_forcing_data_for_ensemble(ds_in, ensemble_member, TIME_SHIFT_HOURS)
    else:
        raise ValueError("Ensemble member must be between 2 and 7")

    # Choose filename
    PATTERN = r"^ngen\.t\d{2}z\.medium_range\.forcing\.f001_f240\.VPU_\d+\.nc$"
    input_file = Path(args.input_file_ens0).name
    if re.match(PATTERN, input_file):
        OUT_FILENAME = input_file.replace("f001_f240", "f001_f204")
    else:
        OUT_FILENAME = "forcings_ens_" + str(ensemble_member) + ".nc"
    out_path = Path(args.output_dir) / OUT_FILENAME

    # Create data array for make_forcing_netcdf
    nvar = len(ds_in.variables) - 2  # Exclude the time and catchment ids variable
    ncat = ds_in["UGRD_10maboveground"].shape[0]
    data_array = np.ones((ncat, 204, nvar), dtype="float64")
    ds_vars = list(ds_mod.keys())
    ds_vars.remove("Time")
    ds_vars.remove("ids")
    for j, jvar in enumerate(ds_vars):
        data_array[:, :, j] = ds_mod[jvar].values

    # create netcdf and write it
    make_forcing_netcdf(
        out_path,
        catchments=ds_mod.ids.values,
        t_ax=ds_mod.Time.values[0, :],
        input_array=data_array,
    )
