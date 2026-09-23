"""The workflow steps.

Each function here says what the step is for. How it is accomplished lives one layer down: in the
Mode bound to the configuration, in the kernels, or in the utility modules. This is the last layer
that reads configuration; below it, functions take data.
"""

import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from forcingprocessor import metadata
from forcingprocessor.config import build_output_layout, read_config, write_run_manifest
from forcingprocessor.plot_forcings import plot_ngen_forcings
from forcingprocessor.records import (
    Extracted,
    Geometry,
    NWMFileMetadata,
    OutputLayout,
    RunConfig,
    WriteResult,
)
from forcingprocessor.utils import Profiler, convert_url2key, ngen_variables, phase

TIME_FMT = "%Y-%m-%d %H:%M:%S"
FRAME_OUTPUTS = ["csv", "parquet", "tar"]


def _fp_animation_and_filenames(cfg: RunConfig) -> None:
    msg = "\nForcingProcessor has awoken. Let's do this."
    for x in msg:
        print(x, end="")
        sys.stdout.flush()
        time.sleep(0.05)
    print("\n")
    print("NWM file names:")
    for jfile in cfg.nwm_forcing_files:
        print(f"{jfile}")


def configure(
    conf: dict, profiler: Profiler
) -> tuple[RunConfig, OutputLayout, NWMFileMetadata]:
    """Read the configuration, bind the run's Mode, and lay out where outputs go.

    Args:
        conf (dict): forcingprocessor config file read as a dict.
        profiler (Profiler): This run's profiling log and timings.

    Returns:
        tuple[RunConfig, OutputLayout, NWMFileMetadata]: The run configuration, its output paths,
            and the information parsed out of the NWM file names.
    """
    with phase("CONFIGURATION", profiler):
        cfg = read_config(conf)
        layout = build_output_layout(cfg)
        nwm_meta = cfg.mode.parse_filenames(cfg.nwm_forcing_files)

    if cfg.ii_verbose:
        _fp_animation_and_filenames(cfg)

    return cfg, layout, nwm_meta


def load_geometry(cfg: RunConfig, profiler: Profiler) -> Geometry:
    """Read the catchment weights, nexus map, or restart mappings this run extracts against."""
    return cfg.mode.load_geometry(cfg, profiler)


def store_inputs(
    cfg: RunConfig, layout: OutputLayout, geom: Geometry, profiler: Profiler
):
    """Archive the inputs that produced this run alongside its outputs.

    Returns:
        None | S3.Client: The s3 client, reused for metadata writes, or None for local runs.
    """
    with phase("STORE_INPUTS", profiler):
        return write_run_manifest(cfg, layout, geom.weights_df)


def extract(cfg: RunConfig, geom: Geometry, profiler: Profiler) -> Extracted:
    """Pull the requested data out of the NWM files."""
    if cfg.ii_verbose:
        print("Entering data extraction...\n", flush=True)

    with phase("PROCESSING", profiler):
        extracted = cfg.mode.extract(cfg, geom)

    if cfg.ii_verbose:
        t_extract = profiler.timings["PROCESSING"]
        complexity = (len(cfg.nwm_forcing_files) * geom.ncatchments) / 10000
        print(
            f"Data extract processes: {cfg.nprocs:.2f}\nExtract time: {t_extract:.2f}"
            + f"\nComplexity: {complexity:.2f}\nScore: {complexity / t_extract:.2f}\n",
            end=None,
            flush=True,
        )

    return extracted


def order_forward_in_time(
    extracted: Extracted, nwm_meta: NWMFileMetadata
) -> tuple[Extracted, NWMFileMetadata]:
    """Ensure the extracted data is written out with time moving forward.

    Analysis and assimilation file lists run backwards in time. Reverse the data so the writers
    always see time moving forward, and swap the lead labels with it so output filenames still
    describe the range they cover. Both are returned rather than mutated in place.

    Args:
        extracted (Extracted): Data extracted from the NWM files.
        nwm_meta (NWMFileMetadata): Information parsed out of the NWM file names.

    Returns:
        tuple[Extracted, NWMFileMetadata]: The data and metadata, ordered forward in time.
    """
    t_ax = extracted.t_ax
    if not t_ax:
        return extracted, nwm_meta

    first = datetime.strptime(t_ax[0], TIME_FMT).replace(tzinfo=UTC)
    last = datetime.strptime(t_ax[-1], TIME_FMT).replace(tzinfo=UTC)
    if first <= last:
        return extracted, nwm_meta

    return (
        replace(
            extracted,
            t_ax=list(reversed(t_ax)),
            data_array=np.flip(extracted.data_array, axis=0),
        ),
        replace(nwm_meta, lead_start=nwm_meta.lead_end, lead_end=nwm_meta.lead_start),
    )


def _writes_frames(cfg: RunConfig) -> bool:
    """Per catchment frames are needed for direct output, for tar, and for statistics."""
    return (
        cfg.ii_plot
        or cfg.ii_collect_stats
        or any(x in cfg.output_file_type for x in FRAME_OUTPUTS)
    )


def write_outputs(
    cfg: RunConfig,
    layout: OutputLayout,
    geom: Geometry,
    nwm_meta: NWMFileMetadata,
    extracted: Extracted,
    profiler: Profiler,
) -> WriteResult:
    """Write the extracted data out in every requested file type."""
    # Imported here to keep writers.py free of a dependency on this module.
    from forcingprocessor.writers import (  # pylint: disable=import-outside-toplevel
        multiprocess_write_df,
    )

    written = WriteResult()
    with phase("FILEWRITING", profiler):
        if "netcdf" in cfg.output_file_type:
            written.netcdf_file_sizes_MB = cfg.mode.write_netcdf(
                cfg, layout, geom, nwm_meta, extracted
            )
        if cfg.ii_verbose:
            print(
                f"Writing catchment forcings to {layout.output_path}!",
                end=None,
                flush=True,
            )

        catchments = cfg.mode.catchment_ids(geom)
        if not _writes_frames(cfg):
            pass
        elif catchments is None:
            print(f"Dataframes don't get written for {cfg.mode.name}")
        else:
            (
                written.forcing_cat_ids,
                written.filenames,
                written.cat_file_sizes_MB,
                written.cat_file_sizes_zipped_MB,
                written.tar_buffs,
            ) = multiprocess_write_df(
                cfg,
                extracted.data_array,
                extracted.t_ax,
                catchments,
                layout.forcing_path,
            )

    if cfg.ii_verbose:
        write_time = profiler.timings["FILEWRITING"]
        print(
            f"\n\nWrite processs: {cfg.nprocs}\nWrite time: {write_time:.2f}"
            + f"\nWrite rate {geom.ncatchments / write_time:.2f} files/second\n",
            end=None,
            flush=True,
        )
    return written


def plot_outputs(
    cfg: RunConfig,
    layout: OutputLayout,
    extracted: Extracted,
    written: WriteResult,
) -> None:
    """Generate the side-by-side GIF comparing NWM and NGEN forcing data.

    Raises:
        TypeError: Raised when extracted is not configured properly.
    """
    if not cfg.ii_plot:
        return
    if written.forcing_cat_ids is None:
        raise TypeError("Plotting only supported for forcings")
    if cfg.gpkg_files[0].endswith(".parquet"):
        print("Plotting currently not implemented for parquet, need geopackage")
        return
    if len(cfg.gpkg_files) > 1:
        print(f"Plotting only the first geopackage {cfg.gpkg_files[0]}")

    cat_ids = ["cat-" + x for x in written.forcing_cat_ids]
    jplot_vars = np.array(
        [
            x
            for x in range(len(ngen_variables))
            if ngen_variables[x] in cfg.ngen_vars_plot
        ]
    )
    if cfg.storage_type == "s3":
        gif_out = Path("./GIFs")
    else:
        gif_out = Path(layout.meta_path, "GIFs")

    if (
        extracted.nwm_data is None
        or extracted.data_array is None
        or extracted.t_ax is None
    ):
        raise TypeError(
            "extracted.nwm_data, extracted.data_array, and extracted.t_ax must not be None"
        )
    if not isinstance(extracted.data_array, np.ndarray):
        raise TypeError("extracted.data_array must be an np.ndarray")

    plot_ngen_forcings(
        extracted.nwm_data,
        extracted.data_array[:, jplot_vars, :],
        cfg.gpkg_files[0],
        extracted.t_ax,
        cat_ids,
        cfg.ngen_vars_plot,
        gif_out,
    )
    if cfg.storage_type == "s3":
        subprocess.run(
            ["aws", "s3", "sync", "./GIFs", f"{layout.meta_path}/GIFs"], check=True
        )


def collect_metadata(
    cfg: RunConfig,
    layout: OutputLayout,
    s3_client,
    geom: Geometry,
    extracted: Extracted,
    written: WriteResult,
    runtime: float,
    profiler: Profiler,
) -> None:
    """Calculate and write this run's statistics next to its outputs."""
    if not cfg.ii_collect_stats:
        return
    with phase("COLLECT_STATS", profiler):
        metadata.collect_metadata(
            cfg, layout, s3_client, geom, extracted, written, runtime
        )


def write_tarballs(
    cfg: RunConfig,
    layout: OutputLayout,
    geom: Geometry,
    written: WriteResult,
    profiler: Profiler,
) -> None:
    """Bundle the written frames into tarballs, for the run types that support it."""
    # Imported here to keep writers.py free of a dependency on this module.
    from forcingprocessor.writers import (  # pylint: disable=import-outside-toplevel
        multiprocess_write_tar,
    )

    if "tar" not in cfg.output_file_type:
        return

    chunks = cfg.mode.tar_chunks(geom)
    if chunks is None:
        print(
            f"TAR file writing is not implemented for {cfg.mode.name}, "
            + "skipping tarball creation"
        )
        return

    with phase("TAR", profiler):
        if cfg.ii_verbose:
            print("\nWriting tarball...", flush=True)
        multiprocess_write_tar(
            cfg,
            layout.forcing_path,
            chunks,
            written.filenames,
            written.tar_buffs,
        )


def print_summary(layout: OutputLayout, timings: dict, t_start: float) -> None:
    """Print a summary of the run and its timings to the console."""
    print("\n\n--------SUMMARY-------")
    msg = f"\nData has been written to {layout.output_path}"
    if "READWEIGHTS" in timings and "CALC_WINDOW" in timings:
        msg += (
            f"\nCalc weights  : {timings['READWEIGHTS'] + timings['CALC_WINDOW']:.2f}s"
        )
    msg += f"\nProcess data  : {timings['PROCESSING']:.2f}s"
    msg += f"\nWrite data    : {timings['FILEWRITING']:.2f}s"
    if "COLLECT_STATS" in timings:
        msg += f"\nCollect stats : {timings['COLLECT_STATS']:.2f}s"
    if "TAR" in timings:
        msg += f"\nWrite tar     : {timings['TAR']:.2f}s"

    runtime = time.perf_counter() - t_start
    msg += f"\nRuntime       : {runtime:.2f}s\n"
    print(msg)


def archive_profile_log(
    cfg: RunConfig, layout: OutputLayout, s3_client, profiler: Profiler
) -> None:
    """Move the profile log in next to the run's metadata."""
    profiler.log("FORCINGPROCESSOR_END")

    if cfg.storage_type == "s3":
        bucket, key = convert_url2key(layout.metaf_path, cfg.storage_type)
        if s3_client is not None:
            s3_client.upload_file(profiler.log_file, bucket, key + "/profile_fp.txt")
        os.remove(profiler.log_file)
    else:
        shutil.move(profiler.log_file, Path(layout.metaf_path, "profile_fp.txt"))
