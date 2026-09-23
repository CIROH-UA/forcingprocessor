"""Utility functions to write data to S3 or local storage in various formats (CSV, Parquet, NetCDF,
tar).

This module knows file formats and storage backends. It knows nothing about which kind of run
produced the data; everything that varies by run type arrives as the bound Mode.
"""

import concurrent.futures as cf
import gzip
import os
import re
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO, TextIOWrapper
from pathlib import Path

import boto3
import numpy as np
import pandas as pd

from forcingprocessor.records import Mode
from forcingprocessor.utils import (
    B2MB,
    convert_url2key,
    distribute_work,
    load_balance,
    make_forcing_netcdf,
    report_usage,
)

FRAME_TYPES = ("csv", "parquet")
WRITE_INTERVAL = 400


@dataclass
class WriteJob:
    """Everything one dataframe write worker needs. Must be picklable."""

    data: np.ndarray
    t_ax: list
    catchments: list
    out_path: str | Path
    storage_type: str
    output_file_type: list
    ntasked: int
    mode: Mode
    ii_verbose: bool = False
    ii_print: bool = False


def write_df(
    df: pd.DataFrame,
    filename: str,
    storage_type: str,
    csv_options: dict | None = None,
    client: boto3.client = None,  # type: ignore
    bucket: str = "",
    key_prefix: str = "",
    local_path: str | Path = "",
):
    """
    Write a DataFrame to S3 or local storage as a CSV or Parquet file.
    The file type is inferred from the filename extension.

    Args:
        df (pd.DataFrame): DataFrame to write.
        filename (str): Name of the file (e.g., 'metadata.csv' or 'metadata.parquet').
        storage_type (str): 's3' or 'local'.
        csv_options (dict | None): Keyword arguments handed to DataFrame.to_csv, carrying whatever
            the consuming model needs. t-route, for instance, reads its inputs headerless.
            Defaults to {"index": False}.
        client (boto3.client, optional): S3 client if using S3.
        bucket (str, optional): S3 bucket name.
        key_prefix (str, optional): S3 key prefix (folder path).
        local_path (str, optional): Local directory path.
    """
    if csv_options is None:
        csv_options = {"index": False}

    ext = Path(filename).suffix.lower()
    if ext not in (".csv", ".parquet"):
        raise ValueError("Only CSV and Parquet output is supported by write_df")

    if storage_type == "s3":
        buf = BytesIO()
        if ext == ".csv":
            df.to_csv(buf, **csv_options)
        else:
            df.to_parquet(buf)
        if client is not None:
            client.put_object(
                Bucket=bucket, Key=f"{key_prefix}/{filename}", Body=buf.getvalue()
            )
        buf.close()
    else:
        out_path = Path(local_path, filename)
        if ext == ".csv":
            df.to_csv(out_path, **csv_options)
        else:
            df.to_parquet(out_path)


def _frame_type(output_file_type: list) -> str | None:
    """Which of csv or parquet the per catchment frames are written as, if either."""
    return next((x for x in output_file_type if x in FRAME_TYPES), None)


def _probe_file_sizes(df: pd.DataFrame, stem: str) -> tuple[float, float]:
    """Plain and gzipped size of one frame, measured by writing it once.

    Metadata reports these for the whole run, so a single sample is enough.
    """
    filename = f"./{stem}.csv"
    df.to_csv(filename, index=False)
    file_size_MB = os.path.getsize(filename) / B2MB
    os.remove(filename)

    filename_zip = re.sub(r"\.\w+$", ".zip", filename)
    with gzip.GzipFile(filename_zip, mode="w") as zipped_file:
        df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False)
    file_zipped_size_MB = os.path.getsize(filename_zip) / B2MB
    os.remove(filename_zip)

    return file_size_MB, file_zipped_size_MB


def _write_data_df(job: WriteJob) -> tuple[list, list, list, list, list]:
    """
    Write catchment forcing data to csv or parquet if requested. Also responsible for
    creating/formatting data in memory for tar writing and metadata collection.

    Args:
        job (WriteJob): This worker's share of the catchments and everything needed to write them.

    Returns:
        forcing_cat_ids: List of catchment identifiers
        filenames: List of filenames
        file_size_MB: List containing the size of each file in MB
        file_zipped_size_MB: List containing the size of each zipped file in MB
        tar_buffs: List of BytesIO buffer objects of data. This is precalculated for performance.
    """
    s3_client = boto3.session.Session().client("s3")  # type: ignore
    nfiles = len(job.catchments)
    pid = os.getpid()
    forcing_cat_ids = []
    tar_buffs = []
    filenames = []
    t_df = 0
    bucket = ""
    key_prefix = ""
    if job.storage_type == "s3":
        bucket, key_prefix = convert_url2key(job.out_path, job.storage_type)  # type: ignore

    df_ext = _frame_type(job.output_file_type)
    t00 = time.perf_counter()
    file_size_MB = 0
    file_zipped_size_MB = 0

    for j, jcatch in enumerate(job.catchments):
        t0 = time.perf_counter()
        df, stem, record_id = job.mode.build_frame(job.data, job.t_ax, j, jcatch)
        t_df += time.perf_counter() - t0
        if record_id is not None:
            forcing_cat_ids.append(record_id)

        if df_ext is not None:
            filename = f"{stem}.{df_ext}"
            if j == 0 and job.ii_verbose:
                print(
                    f"{pid} writing {nfiles} dataframes to {df_ext}",
                    end=None,
                    flush=True,
                )
            kwargs = (
                {"client": s3_client, "bucket": bucket, "key_prefix": key_prefix}
                if job.storage_type == "s3"
                else {"local_path": job.out_path}
            )
            write_df(df, filename, job.storage_type, job.mode.csv_options, **kwargs)  # type: ignore
        else:
            filename = f"{stem}.csv"

        filenames.append(str(Path(filename).name))

        if "tar" in job.output_file_type:
            buf = BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            tar_buffs.append(buf)

        if j == 0:
            file_size_MB, file_zipped_size_MB = _probe_file_sizes(df, stem)

        if (
            job.ii_print
            and job.ii_verbose
            and ((j + 1) % WRITE_INTERVAL == 0 or j == nfiles - 1)
        ):
            t_accum = time.perf_counter() - t00
            rate = (j + 1) * job.ntasked / t_accum
            bytes2bits = 8
            bandwidth_Mbps = rate * file_size_MB * job.ntasked * bytes2bits
            estimate_total_time = nfiles * job.ntasked / rate
            report_usage()
            msg = (
                f"\n{(j + 1) * job.ntasked} dataframes converted out of "
                + f"{nfiles * job.ntasked}\n"
            )
            msg += f"rate             {rate:.2f} files/s\n"
            msg += f"df conversion    {t_df:.2f}s\n"
            msg += f"estimated total write time {estimate_total_time:.2f}s\n"
            msg += f"progress                   {(j + 1) / nfiles * 100:.2f}%\n"
            msg += f"Bandwidth (all processes)   {bandwidth_Mbps:.2f} Mbps"
            print(msg, flush=True)

    return forcing_cat_ids, filenames, [file_size_MB], [file_zipped_size_MB], tar_buffs


def multiprocess_write_df(cfg, data, t_ax, catchments, out_path):
    """
    Sets up the process pool for _write_data_df.

    Parameters:
        cfg (RunConfig): Run configuration.
        data (numpy.ndarray): 3D array containing the data to be written.
        t_ax (numpy.ndarray): Array representing the time axis of the data.
        catchments (iterable): List of catchment identifiers.
        out_path (str): Path where the output files will be saved.

    Returns:
        flat_ids (list): Flattened list of catchment identifiers.
        flat_filenames (list): Flattened list of filenames.
        flat_file_sizes (list): Flattened list of file sizes in MB.
        flat_file_sizes_zipped (list): Flattened list of file sizes after compression in MB.
        flat_tar (list): Flattened list of BytesIO buffers for the tar step.
    """
    catchments = list(catchments)
    catchments_per_proc = distribute_work(catchments, cfg.nprocs)
    catchments_per_proc = load_balance(catchments_per_proc, cfg.ii_verbose)
    ntasked = len(np.nonzero(catchments_per_proc)[0])

    jobs = []
    start = 0
    for i, count in enumerate(catchments_per_proc):
        end = min(start + count, len(catchments))
        jobs.append(
            WriteJob(
                data=cfg.mode.slice_catchments(data, start, end),
                t_ax=t_ax,
                catchments=catchments[start:end],
                out_path=out_path,
                storage_type=cfg.storage_type,
                output_file_type=cfg.output_file_type,
                ntasked=ntasked,
                mode=cfg.mode,
                ii_verbose=cfg.ii_verbose,
                ii_print=(i == ntasked - 1),
            )
        )
        start = end

    with cf.ProcessPoolExecutor(max_workers=cfg.nprocs) as pool:
        results = list(pool.map(_write_data_df, jobs))
    print("\n\nGathering data from write processes...")

    return (
        [x for r in results for x in r[0]],
        [x for r in results for x in r[1]],
        [x for r in results for x in r[2]],
        [x for r in results for x in r[3]],
        [x for r in results for x in r[4]],
    )


def _write_tar(tar_buffs, jcatchunk, catchments, filenames, storage_type, forcing_path):
    """
    Write DataFrames to a tar archive and upload to S3 or save locally as a compressed tar file.

    Args:
        tar_buffs: List of BytesIO buffer objects of data. This is precalculated for performance.
        jcatchunk: Identifier for the chunk of catchments.
        catchments: List of catchments.
        filenames: List of filenames corresponding to the DataFrames.
        storage_type: string s3 or local
        forcing_path: string s3 uri or local path

    Returns:
        None
    """
    print(f"Writing {jcatchunk} tar")
    if storage_type == "s3":
        tar_name = f"{jcatchunk}_forcings.tar.gz"
        buffer = BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as jtar:
            for j, _ in enumerate(catchments):
                jbuff = tar_buffs[j]
                jfilename = filenames[j]
                info = tarfile.TarInfo(name=jfilename)
                info.size = len(jbuff.getbuffer())
                jtar.addfile(info, jbuff)

        print(f"Uploading {jcatchunk} tar to s3")
        buffer.seek(0)
        bucket, key = convert_url2key(forcing_path, storage_type)
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=key + "/" + tar_name, Body=buffer.getvalue())
    else:
        tar_name = Path(forcing_path, f"{jcatchunk}_forcings.tar.gz")
        with tarfile.open(tar_name, "w:gz") as jtar:
            for j, _ in enumerate(catchments):
                jbuff = tar_buffs[j]
                jfilename = filenames[j]
                info = tarfile.TarInfo(name=jfilename)
                info.size = len(jbuff.getbuffer())
                jtar.addfile(info, jbuff)


def multiprocess_write_tar(cfg, forcing_path, catchments, filenames, tar_buffs):
    """
    Write DataFrames to tar archives using multiprocessing.

    Args:
        cfg (RunConfig): Run configuration.
        forcing_path: string s3 uri or local path
        catchments: Dictionary containing catchment chunks.
        filenames: List of filenames corresponding to the DataFrames.
        tar_buffs: List of BytesIO buffer objects of data. This is precalculated for performance.

    Returns:
        None
    """
    i = 0
    k = 0
    tar_buffs_list = []
    jcatchunk_list = []
    catchments_list = []
    filenames_list = []
    for _, jchunk in enumerate(catchments):
        ncatchments = len(catchments[jchunk])
        k += ncatchments
        tar_buffs_list.append(tar_buffs[i:k])
        jcatchunk_list.append(jchunk)
        catchments_list.append(catchments[jchunk])
        filenames_list.append(filenames[i:k])
        i = k

    njobs = len(catchments)

    with cf.ProcessPoolExecutor(max_workers=min(njobs, cfg.nprocs)) as pool:
        for _ in pool.map(
            _write_tar,
            tar_buffs_list,
            jcatchunk_list,
            catchments_list,
            filenames_list,
            [cfg.storage_type for x in range(njobs)],
            [forcing_path for x in range(njobs)],
        ):
            pass


def _write_netcdf(
    data: np.ndarray,
    t_ax: list,
    catchments: list,
    prefix: str,
    filename: str,
    storage_type: str,
):
    """
    Write 3D array data to a NetCDF file.

    Parameters:
        data (numpy.ndarray): 3D array with dimensions (time, forcing_variable, catchment-id).
        t_ax (list): list representing time axis.
        catchments (list): list containing catchment IDs.
        filename (str): string for the filename
    Returns:
        None
    """
    if storage_type == "s3":
        s3_client = boto3.session.Session().client("s3")  # type: ignore
        nc_filename = prefix + "/" + filename
    else:
        nc_filename = Path(prefix, filename)

    data = np.transpose(data, (2, 0, 1))
    t_utc = np.array(
        [
            datetime.timestamp(
                datetime.strptime(jt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            )
            for jt in t_ax
        ],
        dtype=np.float64,
    )
    catchments_arr = np.array(catchments, dtype="str")
    if storage_type == "s3":
        bucket, key = convert_url2key(nc_filename, "s3")
        with tempfile.NamedTemporaryFile(suffix=".nc") as tmpfile:
            make_forcing_netcdf(tmpfile.name, catchments_arr, t_utc, data)
            netcdf_cat_file_size = os.path.getsize(tmpfile.name) / B2MB
            tmpfile.flush()
            tmpfile.seek(0)
            print(f"Uploading netcdf forcings to S3: bucket={bucket}, key={key}")
            s3_client.upload_file(tmpfile.name, bucket, key)
    else:
        make_forcing_netcdf(nc_filename, catchments_arr, t_utc, data)
        print(f"netcdf has been written to {nc_filename}")
        netcdf_cat_file_size = os.path.getsize(nc_filename) / B2MB
    return netcdf_cat_file_size


def multiprocess_write_netcdf(cfg, forcing_path, nwm_meta, data, jcatchment_dict, t_ax):
    """
    Write netcdf forcings using multiprocessing, one file per VPU.

    Parameters:
        cfg (RunConfig): Run configuration.
        forcing_path: string s3 uri or local path
        nwm_meta (NWMFileMetadata): forecast cycle and lead times parsed from the input filenames.
        data (numpy.ndarray): 3D array with dimensions (catchment-id, time, forcing variable).
        jcatchment_dict (dict): Dictionary containing catchment chunks.
        t_ax (numpy.ndarray): Array representing time axis.

    Returns:
        list[float]: Size in MB of each netcdf written.
    """
    i = 0
    k = 0
    data_list = []
    catchments_list = []
    filenames = []
    for _, jvpu in enumerate(jcatchment_dict):
        ncatchments = len(jcatchment_dict[jvpu])
        k += ncatchments
        data_list.append(data[:, :, i:k])
        catchments_list.append(jcatchment_dict[jvpu])
        if nwm_meta.fcst_cycle is None:
            filenames.append(f"{jvpu}_forcings.nc")
        else:
            filenames.append(
                f"ngen.{nwm_meta.fcst_cycle}z.{nwm_meta.urlbase}.forcing.{nwm_meta.lead_start}_"
                + f"{nwm_meta.lead_end}.{jvpu}.nc"
            )
        i = k

    njobs = len(jcatchment_dict)
    with cf.ProcessPoolExecutor(max_workers=min(njobs, cfg.nprocs)) as pool:
        netcdf_cat_file_sizes = list(
            pool.map(
                _write_netcdf,
                data_list,
                [t_ax for x in range(njobs)],
                catchments_list,
                [forcing_path for x in range(njobs)],
                filenames,
                [cfg.storage_type for x in range(njobs)],
            )
        )

    return netcdf_cat_file_sizes
