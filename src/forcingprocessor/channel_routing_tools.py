"""Channel routing kernel: q_lateral per NGEN nexus, summed from the NWM feature ids that drain to
it. Translates between NWM and NGEN IDs!

    (CHRTOUT dataset, nexus -> NWM id mapping) -> (nnexus, 2) of [nexus id, q_lateral]
"""

import itertools
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import xarray as xr

from forcingprocessor.utils import convert_url2key

B2MB = 1048576


def mapped_nwm_ids(mapping: dict) -> list:
    """Every NWM feature id referenced by the nexus mapping.

    Args:
        mapping (dict): Dictionary of NGEN nexus to NWM feature id maps.

    Returns:
        list: Every NWM feature id the mapping refers to.
    """
    return list(itertools.chain.from_iterable(list(mapping.values())))


def read_qlateral(
    nwm_data: xr.Dataset, nwm_file: str, nwm_ids: list
) -> tuple[dict, str, set]:
    """Read q_lateral for the requested NWM feature ids out of one CHRTOUT file.

    Operational CHRTOUT carries the two runoff terms separately and must be summed; retrospective
    files carry q_lateral directly and date stamp the filename rather than the attributes.

    Args:
        nwm_data (xr.Dataset): An open CHRTOUT file.
        nwm_file (str): The file's name, which carries the timestamp for retrospective data.
        nwm_ids (list): The NWM feature ids the mapping refers to.

    Returns:
        tuple[dict, str, set]: q_lateral per feature id, the model output valid time, and the ids
            actually present in the file.
    """
    try:
        subset = nwm_data.sel(feature_id=nwm_ids)
        valid_nwm_cats = nwm_ids
    except KeyError:
        print(
            f"Some NWM IDs from the mapping are not present in {nwm_file}. Only "
            + "processing available IDs.",
            flush=True,
        )
        feature_ids_in_file = set(nwm_data["feature_id"].values)
        valid_nwm_cats = list(feature_ids_in_file.intersection(nwm_ids))
        subset = nwm_data.sel(feature_id=valid_nwm_cats)

    if "retrospective" in nwm_file:
        t = datetime.strftime(
            datetime.strptime(
                nwm_file.split("/")[-1].split(".")[0], "%Y%m%d%H%M"
            ).replace(tzinfo=UTC),
            "%Y-%m-%d %H:%M:%S",
        )
    else:
        # q_lateral is calculated by adding these two together
        subset["q_lateral"] = subset["qSfcLatRunoff"] + subset["qBucket"]
        time_splt = subset.attrs["model_output_valid_time"].split("_")
        t = time_splt[0] + " " + time_splt[1]

    data_allnwm = dict(zip(subset["feature_id"].values, subset["q_lateral"].values))
    return data_allnwm, t, set(valid_nwm_cats)


def sum_to_nexus(data_allnwm: dict, mapping: dict, valid_nwm_set: set) -> np.ndarray:
    """Sum every contributing NWM feature into its NGEN nexus.

    Args:
        data_allnwm (dict): q_lateral per NWM feature id.
        mapping (dict): Dictionary of NGEN nexus to NWM feature id maps.
        valid_nwm_set (set): The feature ids actually present in the file.

    Returns:
        np.ndarray: Array of shape (nnexus, 2) holding [nexus id, q_lateral].
    """
    data_allngen = {
        ngen_nex: sum(
            data_allnwm[nwm_id] for nwm_id in nwm_ids if nwm_id in valid_nwm_set
        )
        for ngen_nex, nwm_ids in mapping.items()
    }
    return np.array(list(data_allngen.items()))


def write_netcdf_chrt(
    storage_type: str, prefix: Path | str, data: np.ndarray, times: list, name: str
):
    """
    Write channel routing data to a NetCDF file.

    Parameters:
        storage_type (str): s3 or local
        prefix (Path | str): filename prefix
        data (numpy.ndarray): 2D array with dimensions (nexus-id, qlateral).
        times (list): list representing time axis.
        name (str): string for the filename
    Returns:
        netcdf_cat_file_size (list): file size of output netcdf
    """
    if storage_type == "s3":
        s3_client = boto3.session.Session().client("s3")  # type: ignore
        nc_filename = str(prefix) + "/" + name
    else:
        nc_filename = Path(prefix, name)

    time_coord = pd.to_datetime(times)
    feature_ids = data[0, :, 0]
    q_lateral = data[:, :, 1].astype(float)

    ds = xr.Dataset(
        {"q_lateral": (("time", "feature_id"), q_lateral)},
        coords={"time": time_coord, "feature_id": feature_ids},
    )
    if storage_type == "s3":
        bucket, key = convert_url2key(nc_filename, "s3")
        with tempfile.NamedTemporaryFile(suffix=".nc") as tmpfile:
            ds.to_netcdf(tmpfile.name, engine="netcdf4")
            netcdf_cat_file_size = os.path.getsize(tmpfile.name) / B2MB
            tmpfile.flush()
            tmpfile.seek(0)
            print(f"Uploading netcdf forcings to S3: bucket={bucket}, key={key}")
            s3_client.upload_file(tmpfile.name, bucket, key)
    else:
        ds.to_netcdf(nc_filename, engine="netcdf4")
        print(f"netcdf has been written to {nc_filename}")
        netcdf_cat_file_size = os.path.getsize(nc_filename) / B2MB
    return [netcdf_cat_file_size]
