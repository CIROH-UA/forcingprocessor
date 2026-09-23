"""Tools to handle configuration data."""

import json
import os
import shutil
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd

from forcingprocessor.modes import select_mode
from forcingprocessor.records import (
    NWMFileMetadata,
    OutputLayout,
    RunConfig,
)
from forcingprocessor.utils import (
    convert_url2key,
    ngen_variables,
    normalize_vpu_id,
)

# Re-exported so that `from forcingprocessor.config import RunConfig` keeps working.
__all__ = [
    "FILE_TYPES",
    "NWMFileMetadata",
    "OutputLayout",
    "RunConfig",
    "build_output_layout",
    "read_config",
    "write_run_manifest",
]

FILE_TYPES = ["csv", "parquet", "tar", "netcdf"]


def read_config(conf: dict) -> RunConfig:
    """
    Parse and validate a forcingprocessor config into a RunConfig.

    This is where the run type is resolved. The resulting RunConfig carries a bound Mode, so no
    step below this one has to work out what kind of run it is in.

    Args:
        conf (dict): forcingprocessor config file
        https://github.com/CIROH-UA/forcingprocessor/blob/main/configs/conf_fp.json read as a dict

    Raises:
        ValueError: Raised when the number of VPU IDs is not the same as the number of passed
            geopackages.
        RuntimeError: Raised when plotting is turned on for a channel routing or a restart run.

    Returns:
        RunConfig: Information from the config file.
    """
    forcing = conf["forcing"]
    gpkg_file = forcing.get("gpkg_file", None)
    gpkg_files = gpkg_file if isinstance(gpkg_file, list) else [gpkg_file]

    # Issue 9: optional explicit VPU ids for multi-gpkg / multi-weight runs.
    # If forcing.vpu_id is not supplied, infer ids from filenames.
    vpu_ids = forcing.get("vpu_id", None)
    if vpu_ids is None:
        vpu_ids = gpkg_files
    elif not isinstance(vpu_ids, list):
        vpu_ids = [vpu_ids]
    vpu_ids = [normalize_vpu_id(x) for x in vpu_ids]
    if len(vpu_ids) != len(gpkg_files):
        raise ValueError(
            "Length of forcing.vpu_id must match length of forcing.gpkg_file"
        )

    map_file = forcing.get("map_file", None)
    restart_map_file = forcing.get("restart_map_file", None)
    mode = select_mode(map_file=map_file, restart_map_file=restart_map_file)

    nwm_file = forcing.get("nwm_file", "")
    with open(nwm_file, "r", encoding="utf-8") as fp:
        nwm_forcing_files = [jline.strip() for jline in fp]

    output_path = conf["storage"].get("output_path", "")
    output_file_type = conf["storage"].get("output_file_type", ["csv"])
    for jtype in output_file_type:
        assert jtype in FILE_TYPES, (
            f"{jtype} for output_file_type is not accepted! Accepted: {FILE_TYPES}"
        )
    assert not ("parquet" in output_file_type and "csv" in output_file_type), (
        "Both parquet and csv cannot be simultaneously specified in output_file_type, pick one."
    )

    if "s3://" in output_path:
        storage_type = "s3"
    elif "google" in output_path:
        storage_type = "google"
    else:
        storage_type = "local"

    first_file = nwm_forcing_files[0]
    if "s3://" in first_file:
        fs_type = "s3"
    elif any(x in first_file for x in ["google", "gs://", "gcs://"]):
        fs_type = "google"
    else:
        fs_type = None

    plot = conf.get("plot", None)
    if plot:
        if not mode.supports_plotting:
            raise RuntimeError(
                "Plotting not supported for channel routing or restart processing."
            )
        nts_plot = plot.get("nts_plot", 10)
        ngen_vars_plot = plot.get("ngen_vars", ngen_variables)
    else:
        nts_plot = 0
        ngen_vars_plot = []

    cpu_count = os.cpu_count()
    if cpu_count is None:
        cpu_count = 1

    return RunConfig(
        conf=conf,
        mode=mode,
        gpkg_files=gpkg_files,
        vpu_ids=vpu_ids,
        nwm_file=nwm_file,
        nwm_forcing_files=nwm_forcing_files,
        map_file=map_file,
        restart_map_file=restart_map_file,
        crosswalk_file=forcing.get("crosswalk_file", None),
        routelink_file=forcing.get("routelink_file", None),
        output_path=output_path,
        output_file_type=output_file_type,
        storage_type=storage_type,
        fs_type=fs_type,
        nprocs=conf["run"].get("nprocs", int(cpu_count * 0.5)),
        ii_verbose=conf["run"].get("verbose", False),
        ii_collect_stats=conf["run"].get("collect_stats", True),
        ii_plot=bool(plot),
        nts_plot=nts_plot,
        ngen_vars_plot=ngen_vars_plot,
    )


def build_output_layout(cfg: RunConfig) -> OutputLayout:
    """
    Resolve the output directory tree, creating it for local runs.

    Args:
        cfg (RunConfig): forcingprocessor configuration information.

    Raises:
        TypeError: Raised if cfg is configured incorrectly

    Returns:
        OutputLayout: Information on the path(s) where output files should be written.
    """
    output_path = cfg.output_path

    if cfg.storage_type != "local":
        return OutputLayout(
            output_path=output_path,
            forcing_path=output_path,
            meta_path=output_path + "/metadata",
            metaf_path=output_path + "/metadata/forcings_metadata",
        )

    if output_path == "":
        datentime = datetime.now(UTC).strftime("%m%d%y_%H%M%S")
        output_path = os.path.join(os.getcwd(), datentime)
    output_path = Path(output_path)
    layout = OutputLayout(
        output_path=output_path,
        forcing_path=Path(output_path, *cfg.mode.forcing_subdir),
        meta_path=Path(output_path, "metadata"),
        metaf_path=Path(output_path, "metadata", "forcings_metadata"),
    )
    for jpath in [
        layout.output_path,
        layout.forcing_path,
        layout.meta_path,
        layout.metaf_path,
    ]:
        if isinstance(jpath, Path):
            jpath.mkdir(parents=True, exist_ok=True)
        else:
            raise TypeError("The paths in layout must be Path objects.")
    return layout


def write_run_manifest(
    cfg: RunConfig, layout: OutputLayout, weights_df: pd.DataFrame | None = None
):
    """Store the inputs that produced this run alongside its outputs. Returns the
    s3 client used, which is reused for metadata writes, or None for local runs.

    Args:
        cfg (RunConfig): forcingprocessor configuration information.
        layout (OutputLayout): Information on the path(s) where outpul files should be written.
        weights_df (pd.DataFrame | None, optional): Dataframe of catchment weights. Defaults to
            None.

    Returns:
        None | S3.Client: Filesystem. None if not S3.
    """
    if cfg.storage_type == "local":
        with open(Path(layout.metaf_path, "conf.json"), "w", encoding="utf-8") as f:
            json.dump(cfg.conf, f, indent=4)
        shutil.copy(cfg.nwm_file, layout.metaf_path)
        if weights_df is not None:
            weights_df.to_parquet(Path(layout.metaf_path, "weights.parquet"))
        return None

    if cfg.storage_type != "s3":
        return None

    bucket, key = convert_url2key(layout.metaf_path, cfg.storage_type)
    s3 = boto3.client("s3")
    s3.put_object(
        Body=json.dumps(cfg.conf, indent=4), Bucket=bucket, Key=f"{key}/conf_fp.json"
    )
    s3.upload_file(cfg.nwm_file, bucket, f"{key}/{os.path.basename(cfg.nwm_file)}")
    if weights_df is not None:
        buf = BytesIO()
        weights_df.to_parquet(buf, index=False)
        buf.seek(0)
        s3.put_object(Bucket=bucket, Key=f"{key}/weights.parquet", Body=buf.getvalue())
    return s3
