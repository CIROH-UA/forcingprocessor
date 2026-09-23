"""Functions for calculating and writing metadata.

The universal part, input and output file sizes, lives here. Anything shaped by the run type comes
from the Mode bound to the configuration.
"""

import numpy as np
import pandas as pd

from forcingprocessor.records import (
    Extracted,
    Geometry,
    OutputLayout,
    RunConfig,
    WriteResult,
)
from forcingprocessor.utils import convert_url2key
from forcingprocessor.writers import write_df


def summarize_sizes(
    sizes: np.ndarray | list | None,
) -> tuple[float, float, np.ndarray | np.float32 | np.float64 | float]:
    """Given a list of file sizes, calculate the average, median, and standard deviation."""
    if sizes is None or len(sizes) == 0:
        return 0, 0, 0
    sizes = np.fromiter(sizes, dtype=float)
    return np.average(sizes), np.median(sizes), np.std(sizes)


def size_summary(
    cfg: RunConfig,
    extracted: Extracted,
    written: WriteResult,
    runtime: float,
    nvars_in: int = 1,
    nvars_out: int = 1,
) -> dict:
    """Summarize input and output file sizes for a run that writes per catchment files.

    Args:
        cfg (RunConfig): forcingprocessor run configuration.
        extracted (Extracted): The extracted data.
        written (WriteResult): The written data.
        runtime (float): The runtime of the run.
        nvars_in (int): Number of input variables read per file. Defaults to 1.
        nvars_out (int): Number of output variables written per catchment. Defaults to 1.

    Returns:
        dict: A dictionary containing the summarized metadata.
    """
    ii_dataframes = "csv" in cfg.output_file_type or "parquet" in cfg.output_file_type
    nwm_avg, nwm_med, nwm_std = summarize_sizes(extracted.nwm_file_sizes_MB)
    cat_avg, cat_med, cat_std = (
        summarize_sizes(written.cat_file_sizes_MB) if ii_dataframes else (0, 0, 0)
    )
    zip_avg, zip_med, zip_std = (
        summarize_sizes(written.cat_file_sizes_zipped_MB)
        if ii_dataframes
        else (0, 0, 0)
    )
    nc_avg, nc_med, nc_std = (
        summarize_sizes(written.netcdf_file_sizes_MB)
        if "netcdf" in cfg.output_file_type
        else (0, 0, 0)
    )

    nfiles = len(cfg.nwm_forcing_files)
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


def collect_metadata(
    cfg: RunConfig,
    layout: OutputLayout,
    s3_client,
    geom: Geometry,
    extracted: Extracted,
    written: WriteResult,
    runtime: float,
) -> None:
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

    metadata_df = pd.DataFrame.from_dict(
        cfg.mode.summarize(cfg, extracted, written, runtime)
    )
    frames = cfg.mode.stat_frames(cfg, geom, extracted, written)
    frames.append((metadata_df, "metadata.csv"))

    if cfg.storage_type == "s3":
        bucket, key = convert_url2key(layout.metaf_path, cfg.storage_type)
        kwargs = {"bucket": bucket, "key_prefix": key, "client": s3_client}
    else:
        kwargs = {"local_path": layout.metaf_path}

    for df, filename in frames:
        if df is None or df.empty:
            continue
        write_df(df, filename, cfg.storage_type, {"index": False}, **kwargs)  # type: ignore
