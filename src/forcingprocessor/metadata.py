"""Functions for calculating and writing metadata."""

import numpy as np
import pandas as pd

from forcingprocessor.utils import convert_url2key, ngen_variables, nwm_variables
from forcingprocessor.writers import write_df
from forcingprocessor.config import RunConfig, OutputLayout
# from forcingprocessor.processor import Extracted, WriteResult, Geometry


def _summarize_sizes(
    sizes: np.ndarray | list | None,
) -> tuple[float, float, np.ndarray | np.float32 | np.float64 | float]:
    """Given a list of file sizes, calculate the average, median, and standard deviation of the file
    sizes.

    Args:
        sizes (np.ndarray | list | None): List of files for which to summarize sizes.

    Returns:
        tuple[float, float, np.ndarray | np.float32 | np.float64 | float]: The average, median, and
            standard deviation of the sizes.
    """
    if sizes is None or len(sizes) == 0:
        return 0, 0, 0
    sizes = np.fromiter(sizes, dtype=float)
    return np.average(sizes), np.median(sizes), np.std(sizes)


def _calculate_vpu_precip_stats(
    data_array: np.ndarray, catchment_ids: list, jcatchment_dict: dict
) -> pd.DataFrame:
    """
    Calculate compact precipitation statistics for each VPU.

    Parameters
    ----------
    data_array : np.ndarray
        Forcing data with dimensions (time, variable, catchment).
    catchment_ids : list
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
        str(catchment_id): i for i, catchment_id in enumerate(catchment_ids)
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


def _build_metadata(
    cfg: RunConfig, runtime: float, extracted, written
) -> dict:
    """Summarize input and output file sizes for this run.

    Args:
        cfg (RunConfig): The run configuration.
        runtime (float): The runtime of the run.
        extracted (Extracted): The extracted data.
        written (WriteResult): The written data.

    Returns:
        dict: A dictionary containing the summarized metadata.
    """
    ii_dataframes = "csv" in cfg.output_file_type or "parquet" in cfg.output_file_type
    nwm_avg, nwm_med, nwm_std = _summarize_sizes(extracted.nwm_file_sizes_MB)
    cat_avg, cat_med, cat_std = (
        _summarize_sizes(written.cat_file_sizes_MB) if ii_dataframes else (0, 0, 0)
    )
    zip_avg, zip_med, zip_std = (
        _summarize_sizes(written.cat_file_sizes_zipped_MB)
        if ii_dataframes
        else (0, 0, 0)
    )
    nc_avg, nc_med, nc_std = (
        _summarize_sizes(written.netcdf_file_sizes_MB)
        if "netcdf" in cfg.output_file_type
        else (0, 0, 0)
    )

    nfiles = len(cfg.nwm_forcing_files)
    if cfg.data_source == "troute_restarts":
        return {
            "runtime_s": [round(runtime, 2)],
            "nwmfiles_input": [nfiles],
            "nwm_file_size": [nwm_avg],
            "netcdf_catch_file_size_MB": [nc_avg],
        }

    nvars_in = len(nwm_variables) if cfg.data_source == "forcings" else 1
    nvars_out = len(ngen_variables) if cfg.data_source == "forcings" else 1
    return {
        "runtime_s": [round(runtime, 2)],
        "nvars_intput": [nvars_in],
        "nwmfiles_input": [nfiles],
        "nwm_file_size_avg_MB": [nwm_avg],
        "nwm_file_size_med_MB": [nwm_med],
        "nwm_file_size_std_MB": [nwm_std],
        "catch_files_output": [nfiles],
        "nvars_output": [nvars_out],
        "individual_catch_file_size_avg_MB": [cat_avg],
        "individual_catch_file_size_med_MB": [cat_med],
        "individual_catch_file_size_std_MB": [cat_std],
        "individual_catch_file_zip_size_avg_MB": [zip_avg],
        "individual_catch_file_zip_size_med_MB": [zip_med],
        "individual_catch_file_zip_size_std_MB": [zip_std],
        "netcdf_catch_file_size_avg_MB": [nc_avg],
        "netcdf_catch_file_size_med_MB": [nc_med],
        "netcdf_catch_file_size_std_MB": [nc_std],
    }


def _build_stat_frames(
    cfg: RunConfig, data_array: np.ndarray, ids: list | None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per catchment averages and medians over the time axis.

    Args:
        cfg (RunConfig): forcingprocessor run configuration
        data_array (np.ndarray): The data array for which to calculate statistics
        ids (list | None): The list of catchment IDs

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the average and median dataframes
    """
    if cfg.data_source == "forcings" and data_array is not None:
        columns = ngen_variables
        id_column = "catchment id"
        avg = np.average(data_array, axis=0)
        med = np.median(data_array, axis=0)
    elif cfg.data_source == "channel_routing":
        columns = ["q_lateral"]
        id_column = "nexus id"
        avg = np.average(data_array[:, :, 1], axis=0)
        med = np.median(data_array[:, :, 1], axis=0)
    else:
        # troute restarts won't need stats calculated for them since there's no time axis
        return pd.DataFrame(), pd.DataFrame()

    avg_df = pd.DataFrame(avg.T, columns=columns)
    avg_df.insert(0, id_column, ids)
    med_df = pd.DataFrame(med.T, columns=columns)
    med_df.insert(0, id_column, ids)
    return avg_df, med_df


def collect_metadata(
    cfg: RunConfig,
    layout: OutputLayout,
    s3_client,
    # geom: Geometry,
    # extracted: Extracted,
    # written: WriteResult,
    geom,
    extracted,
    written,
    runtime: float,
):
    """Calculate and write run statistics next to the run outputs.

    Args:
        cfg (RunConfig): forcingprocessor run configuration
        layout (OutputLayout): output layout configuration
        s3_client (boto3.S3.Client): S3 client for handling S3 operations
        geom (Geometry): geometric information
        extracted (Extracted): extracted data
        written (WriteResult): written results
        runtime (float): runtime in seconds
    """
    if cfg.ii_verbose:
        print("Data processing, now calculating metadata...", flush=True)

    if cfg.data_source == "forcings":
        ids = written.forcing_cat_ids
    elif cfg.data_source == "channel_routing" and geom.nwm_ngen_map is not None:
        ids = list(geom.nwm_ngen_map.keys())
    else:
        ids = None

    metadata_df = pd.DataFrame.from_dict(
        _build_metadata(cfg, runtime, extracted, written)
    )
    if isinstance(extracted.data_array, np.ndarray) and ids is not None:
        avg_df, med_df = _build_stat_frames(cfg, extracted.data_array, ids)
    else:
        avg_df, med_df = pd.DataFrame(), pd.DataFrame()

    frames = [(metadata_df, "metadata.csv")]
    if (
        cfg.data_source == "forcings"
        and isinstance(extracted.data_array, np.ndarray)
        and geom.jcatchment_dict is not None
        and geom.weights_df is not None
    ):
        # Issue 9: write compact VPU-level precipitation statistics.
        vpu_precip_df = _calculate_vpu_precip_stats(
            extracted.data_array, list(geom.weights_df.index), geom.jcatchment_dict
        )
        frames.insert(0, (vpu_precip_df, "metadata_by_vpu.csv"))
    if not avg_df.empty:
        frames.append((avg_df, "catchments_avg.csv"))
    if not med_df.empty:
        frames.append((med_df, "catchments_median.csv"))

    if cfg.storage_type == "s3":
        bucket, key = convert_url2key(layout.metaf_path, cfg.storage_type)
        kwargs = {"bucket": bucket, "key_prefix": key, "client": s3_client}
    else:
        kwargs = {"local_path": layout.metaf_path}

    for df, filename in frames:
        write_df(df, filename, cfg.storage_type, "na", **kwargs)  # type: ignore
