import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3

from forcingprocessor.utils import (
    convert_url2key,
    ngen_variables,
    normalize_vpu_id,
)

FILE_TYPES = ["csv", "parquet", "tar", "netcdf"]

# s3://noaa-nwm-pds/nwm.20241029/forcing_short_range/nwm.t00z.short_range.forcing.f001.conus.nc
FILENAME_PATTERNS = {
    "forcings": r"nwm\.(\d{8})/forcing_(\w+)/nwm\.(\w+)(\d{2})z\.\w+\.forcing\.(\w+)(\d{2})\.conus\.nc",
    "channel_routing": r"nwm\.(\d{8})/(\w+)/nwm\.(\w+)(\d{2})z\.\w+\.channel_rt[^\.]*\.(\w+)(\d{2})\.conus\.nc",
    # s3://noaa-nwm-pds/nwm.20241029/analysis_assim/nwm.t16z.analysis_assim.channel_rt.tm00.conus.nc
    "troute_restarts": r"nwm\.(\d{8})/analysis_assim/nwm\.t(\d{2})z\.analysis_assim\.channel_rt\.tm00\.conus\.nc",
}


@dataclass
class RunConfig:
    conf: dict
    data_source: str
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
    output_path: object
    forcing_path: object
    meta_path: object
    metaf_path: Path


@dataclass
class NWMFileMetadata:
    urlbase: str = ""
    fcst_cycle: str | None = None
    lead_start: str = ""
    lead_end: str = ""
    restart_date: str = ""
    restart_hour: str = ""


def read_config(conf):
    """
    Parse and validate a forcingprocessor config into a RunConfig.

    Inputs: forcingprocessor config file
        https://github.com/CIROH-UA/forcingprocessor/blob/main/configs/conf_fp.json
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
    if map_file:  # NWM to NGEN channel routing processing requires json map
        data_source = "channel_routing"
    elif restart_map_file:
        data_source = "troute_restarts"
    else:
        data_source = "forcings"

    nwm_file = forcing.get("nwm_file", "")
    with open(nwm_file, "r", encoding="utf-8") as fp:
        nwm_forcing_files = [jline.strip() for jline in fp.readlines()]

    output_path = conf["storage"].get("output_path", "")
    output_file_type = conf["storage"].get("output_file_type", ["csv"])
    for jtype in output_file_type:
        assert (
            jtype in FILE_TYPES
        ), f"{jtype} for output_file_type is not accepted! Accepted: {FILE_TYPES}"
    assert not (
        "parquet" in output_file_type and "csv" in output_file_type
    ), "Both parquet and csv cannot be simultaneously specified in output_file_type, pick one."

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
        if data_source != "forcings":
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
        data_source=data_source,
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


def build_output_layout(cfg):
    """
    Resolve the output directory tree, creating it for local runs.
    """
    output_path = cfg.output_path
    if cfg.data_source == "channel_routing":
        forcing_subdir = ("outputs", "ngen")
    elif cfg.data_source == "troute_restarts":
        forcing_subdir = ("restart",)
    else:
        forcing_subdir = ("forcings",)

    if cfg.storage_type != "local":
        return OutputLayout(
            output_path=output_path,
            forcing_path=output_path,
            meta_path=output_path + "/metadata",
            metaf_path=output_path + "/metadata/forcings_metadata",
        )

    if output_path == "":
        datentime = datetime.now(timezone.utc).strftime("%m%d%y_%H%M%S")
        output_path = os.path.join(os.getcwd(), datentime)
    output_path = Path(output_path)
    layout = OutputLayout(
        output_path=output_path,
        forcing_path=Path(output_path, *forcing_subdir),
        meta_path=Path(output_path, "metadata"),
        metaf_path=Path(output_path, "metadata", "forcings_metadata"),
    )
    for jpath in [
        layout.output_path,
        layout.forcing_path,
        layout.meta_path,
        layout.metaf_path,
    ]:
        jpath.mkdir(parents=True, exist_ok=True)
    return layout


def write_run_manifest(cfg, layout, weights_df=None):
    """
    Store the inputs that produced this run alongside its outputs. Returns the
    s3 client used, which is reused for metadata writes, or None for local runs.
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


def parse_nwm_filenames(cfg):
    """
    Extract forecast cycle and lead time from the first and last file names.
    """
    pattern = FILENAME_PATTERNS[cfg.data_source]
    files = cfg.nwm_forcing_files
    meta = NWMFileMetadata()
    match = re.search(pattern, files[0])

    if cfg.data_source == "troute_restarts":
        if match:
            meta.restart_date = match.group(1)
            meta.restart_hour = match.group(2)
        else:
            print("Could not extract restart date and time")
        return meta

    if match:
        meta.urlbase = match.group(2)
        meta.fcst_cycle = match.group(3) + match.group(4)
        meta.lead_start = match.group(5) + match.group(6)
    else:
        print(
            "Could not extract forecast cycle and lead start from the first NWM forcing file: " +
            f"{files[0]}"
        )

    match = re.search(pattern, files[-1])
    if match:
        meta.lead_end = match.group(5) + match.group(6)
    else:
        print(f"Could not extract lead end from the last NWM forcing file: {files[-1]}")

    return meta
