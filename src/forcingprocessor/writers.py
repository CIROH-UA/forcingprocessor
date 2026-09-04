"""Utility functions to write data to S3 or local storage in various formats (CSV, Parquet, NetCDF,
tar)."""

import concurrent.futures as cf
import gzip
import os
import re
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from io import BytesIO, TextIOWrapper
from pathlib import Path

import boto3
import numpy as np
import pandas as pd

from forcingprocessor.utils import (
    B2MB,
    convert_url2key,
    distribute_work,
    load_balance,
    make_forcing_netcdf,
    ngen_variables,
    report_usage,
)


def write_df(
    df: pd.DataFrame,
    filename: str,
    storage_type: str,
    data_source_arg: str,
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
        data_source_arg (str): 'channel_routing' or 'forcings'.
        client (boto3.client, optional): S3 client if using S3.
        bucket (str, optional): S3 bucket name.
        key_prefix (str, optional): S3 key prefix (folder path).
        local_path (str, optional): Local directory path.
    """
    ext = Path(filename).suffix.lower()
    if ext == ".csv":
        if storage_type == "s3":
            buf = BytesIO()
            if data_source_arg == "channel_routing":
                df.to_csv(buf, header=False)  # t-route input format
            else:
                df.to_csv(buf, index=False)

            key_name = f"{key_prefix}/{filename}"
            if client is not None:
                client.put_object(Bucket=bucket, Key=key_name, Body=buf.getvalue())
            buf.close()
        else:
            out_path = Path(local_path, filename)
            if data_source_arg == "channel_routing":
                df.to_csv(out_path, header=False)
            else:
                df.to_csv(out_path, index=False)
    elif ext == ".parquet":
        if storage_type == "s3":
            buf = BytesIO()
            df.to_parquet(buf)
            key_name = f"{key_prefix}/{filename}"
            if client is not None:
                client.put_object(Bucket=bucket, Key=key_name, Body=buf.getvalue())
            buf.close()
        else:
            out_path = Path(local_path, filename)
            df.to_parquet(out_path)
    else:
        raise ValueError("Only CSV and Parquet output is supported by write_df")


def _write_data_df(
    data,
    t_ax,
    catchments,
    out_path,
    ii_print,
    ii_verbose,
    storage_type,
    output_file_type,
    ntasked,
    data_source_arg,
):
    """
    Write catchment forcing data to csv or parquet if requested. Also responsible for
    creating/formatting data in memory for tar writing and metadata collection.

    Args:
        data: Input data to be written (numpy array)
        t_ax: Time axis data (numpy array)
        catchments: List of catchment identifiers
        out_path: Output path for writing files
        ii_print: Flag for printing progress information

    Returns:
        forcing_cat_ids: List of catchment identifiers
        filenames: List of filenames
        file_size_MB: List containing the size of each file in MB
        file_zipped_size_MB: List containing the size of each zipped file in MB
        tar_buffs: List of BytesIO buffer objects of data. This is precalculated for performance.
    """
    s3_client = boto3.session.Session().client("s3")  # type: ignore
    nfiles = len(catchments)
    pid = os.getpid()
    forcing_cat_ids = []
    tar_buffs = []
    filenames = []
    filename = ""
    write_int = 400
    t_df = 0
    bucket = None
    key_prefix = None
    if storage_type == "s3":
        bucket, key_prefix = convert_url2key(out_path, storage_type)

    t00 = time.perf_counter()
    file_size_MB = 0
    file_zipped_size_MB = 0
    for j, jcatch in enumerate(catchments):
        t0 = time.perf_counter()
        if data_source_arg == "forcings":
            df_data = data[:, :, j]
            df = pd.DataFrame(df_data, columns=ngen_variables)
            df.insert(0, "time", t_ax)
        else:
            df_data = data[:, j, :]
            try:
                df = pd.DataFrame(df_data, columns=["feature_id", "q_lateral"])
            except Exception:
                print("data source", data_source_arg)
                raise
            df = df[["q_lateral"]]
            df["time"] = t_ax
            df = df[["time", "q_lateral"]]  # reorder cols to maintain parity
        t_df += time.perf_counter() - t0

        if data_source_arg == "forcings":
            cat_id = jcatch.split("-")[1]
            forcing_cat_ids.append(cat_id)
        else:
            nex_id = jcatch

        df_ext = next((x for x in output_file_type if x in ("parquet", "csv")), None)
        if df_ext is not None:
            if data_source_arg == "forcings":
                filename = f"cat-{cat_id}.{df_ext}"
            else:
                filename = f"{nex_id}.{df_ext}"
            if j == 0 and ii_verbose:
                print(
                    f"{pid} writing {nfiles} dataframes to {df_ext}",
                    end=None,
                    flush=True,
                )
            kwargs = (
                {"client": s3_client, "bucket": bucket, "key_prefix": key_prefix}
                if storage_type == "s3"
                else {"local_path": out_path}
            )
            write_df(df, filename, storage_type, data_source_arg, **kwargs)
        else:
            if data_source_arg == "forcings":
                filename = f"./cat-{cat_id}.csv"
            else:
                filename = f"./{nex_id}.csv"

        filenames.append(str(Path(filename).name))

        if "tar" in output_file_type:
            buf = BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            tar_buffs.append(buf)

        if j == 0:
            if not os.path.exists(filename):
                if data_source_arg == "forcings":
                    filename = f"./cat-{cat_id}.csv"
                else:
                    filename = f"./{nex_id}.csv"
                df.to_csv(filename, index=False)
                file_size_MB = os.path.getsize(filename) / B2MB
                os.remove(filename)
            else:
                file_size_MB = os.path.getsize(filename) / B2MB

            pattern = r"\.\w+$"
            filename_zip = re.sub(pattern, ".zip", filename)
            with gzip.GzipFile(filename_zip, mode="w") as zipped_file:
                df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False)
            file_zipped_size_MB = os.path.getsize(filename_zip) / B2MB
            os.remove(filename_zip)

        if ii_print and ii_verbose and ((j + 1) % write_int == 0 or j == nfiles - 1):
            t_accum = time.perf_counter() - t00
            rate = (j + 1) * ntasked / t_accum
            bytes2bits = 8
            bandwidth_Mbps = rate * file_size_MB * ntasked * bytes2bits
            estimate_total_time = nfiles * ntasked / rate
            report_usage()
            msg = f"\n{(j + 1) * ntasked} dataframes converted out of {nfiles * ntasked}\n"
            msg += f"rate             {rate:.2f} files/s\n"
            msg += f"df conversion    {t_df:.2f}s\n"
            msg += f"estimated total write time {estimate_total_time:.2f}s\n"
            msg += f"progress                   {(j + 1) / nfiles * 100:.2f}%\n"
            msg += f"Bandwidth (all processes)   {bandwidth_Mbps:.2f} Mbps"
            print(msg, flush=True)

    return forcing_cat_ids, filenames, [file_size_MB], [file_zipped_size_MB], tar_buffs


def multiprocess_write_df(cfg, data, t_ax, catchments, out_path):
    """
    Sets up the process pool for write_data_df.

    Parameters:
        cfg (RunConfig): Run configuration.
        data (numpy.ndarray): 3D array containing the data to be written.
        t_ax (numpy.ndarray): Array representing the time axis of the data.
        catchments (iterable): List of catchment identifiers.
        out_path (str): Path where the output files will be saved.

    Returns:
        flat_ids (list): Flattened list of catchment identifiers.
        flat_dfs (list): Flattened list of pandas DataFrames.
        flat_filenames (list): Flattened list of filenames.
        flat_file_sizes (list): Flattened list of file sizes in MB.
        flat_file_sizes_zipped (list): Flattened list of file sizes after compression in MB.
    """
    nprocs = cfg.nprocs
    catchments_per_proc = distribute_work(catchments, nprocs)
    catchments_per_proc = load_balance(catchments_per_proc, cfg.ii_verbose)
    ntasked = len(np.nonzero(catchments_per_proc)[0])

    ncatchments = len(catchments)
    out_path_list = []
    print_list = []
    worker_time_list = []
    worker_data_list = []
    worker_catchment_list = []
    worker_catchments = {}

    i = 0
    count = 0
    start = 0
    end = 0
    ii_print = False
    for j, jcatch in enumerate(catchments):
        worker_catchments[jcatch] = jcatch
        count += 1
        if count == catchments_per_proc[i] or j == ncatchments - 1:
            if len(worker_catchment_list) == ntasked - 1:
                ii_print = True

            end = min(start + catchments_per_proc[i], ncatchments)
            if cfg.data_source == "forcings":
                worker_data = data[:, :, start:end]
            else:
                worker_data = data[:, start:end, :]
            worker_data_list.append(worker_data)
            start = end

            worker_catchment_list.append(worker_catchments)
            out_path_list.append(out_path)
            print_list.append(ii_print)
            worker_time_list.append(t_ax)

            worker_catchments = {}
            count = 0

            i += 1

    ids = []
    filenames = []
    file_sizes_MB = []
    file_sizes_zipped_MB = []
    tar_buffs = []
    with cf.ProcessPoolExecutor(max_workers=nprocs) as pool:
        for results in pool.map(
            _write_data_df,
            worker_data_list,
            worker_time_list,
            worker_catchment_list,
            out_path_list,
            print_list,
            [cfg.ii_verbose for x in range(nprocs)],
            [cfg.storage_type for x in range(nprocs)],
            [cfg.output_file_type for x in range(nprocs)],
            [ntasked for x in range(nprocs)],
            [cfg.data_source for x in range(nprocs)],
        ):
            ids.append(results[0])
            filenames.append(results[1])
            file_sizes_MB.append(results[2])
            file_sizes_zipped_MB.append(results[3])
            tar_buffs.append(results[4])
    print("\n\nGathering data from write processes...")

    flat_ids = []
    flat_filenames = []
    flat_file_sizes = []
    flat_file_sizes_zipped = []
    flat_tar = []

    while ids:
        flat_ids.extend(ids.pop(0))
        flat_filenames.extend(filenames.pop(0))
        flat_file_sizes.extend(file_sizes_MB.pop(0))
        flat_file_sizes_zipped.extend(file_sizes_zipped_MB.pop(0))
        flat_tar.extend(tar_buffs.pop(0))

    return flat_ids, flat_filenames, flat_file_sizes, flat_file_sizes_zipped, flat_tar


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
    Write DataFrames to tar archives using multiprocessing.

    Parameters:
        cfg (RunConfig): Run configuration.
        forcing_path: string s3 uri or local path
        nwm_meta (NWMFileMetadata): forecast cycle and lead times parsed from the input filenames.
        data (numpy.ndarray): 3D array with dimensions (catchment-id, time, forcing variable).
        jcatchment_dict (dict): Dictionary containing catchment chunks.
        t_ax (numpy.ndarray): Array representing time axis.

    Returns:
        None
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
    netcdf_cat_file_sizes = []
    with cf.ProcessPoolExecutor(max_workers=min(njobs, cfg.nprocs)) as pool:
        # for results in pool.map(
        #     _write_netcdf,
        #     data_list,
        #     [t_ax for x in range(njobs)],
        #     catchments_list,
        #     [forcing_path for x in range(njobs)],
        #     filenames,
        #     [cfg.storage_type for x in range(njobs)],
        # ):
        #     netcdf_cat_file_sizes.append(results)
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
