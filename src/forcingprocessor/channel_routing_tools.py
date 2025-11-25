"""Tools to extract and write q_lateral values into a format ingestible by
t-route. Translates between NWM and NGEN IDs!"""

import itertools
import os
from io import BytesIO
import time
from datetime import datetime
from pathlib import Path
import gcsfs
import requests
import xarray as xr
import numpy as np
import pandas as pd
import boto3
from forcingprocessor.utils import convert_url2key, report_usage

B2MB = 1048576

def channelrouting_nwm2ngen(nwm_files: list,
                            mapping_arg: dict,
                            fs_type_arg: str,
                            fs_arg = None,
                            ii_verbose_arg: bool = False
                            ):
    """
    Retrieve catchment level data from national water model files

    Inputs:
    nwm_files (list): list of filenames (urls for remote, local paths otherwise),
    fs_arg (filesystem): an optional file system for cloud storage reads
    mapping_arg (dict): dictionary of NWM to NGEN ID maps
    fs_type_arg (str): type of file system
    ii_verbose_arg (bool): verbosity

    Outputs: [data_list, t_list, nwm_file_sizes_MB]
    data_list (list): list of ngen forcings ordered in time.
    t_list (list): list of model output times
    nwm_file_sizes_MB (list): list of file sizes of input CHRTOUT data
    """
    topen = 0
    txrds = 0
    tfill = 0
    tdata = 0
    t_list = []
    nfiles = len(nwm_files)
    nwm_cats = list(itertools.chain.from_iterable(list(mapping_arg.values())))
    print(nwm_cats, flush=True)
    if fs_type_arg == 'google' :
        fs_arg = gcsfs.GCSFileSystem()
    pid = os.getpid()
    if ii_verbose_arg:
        print(f'Process #{pid} extracting data from {nfiles} files',end=None,flush=True)
    data_list = []
    nwm_file_sizes_MB = []
    for j, nwm_file in enumerate(nwm_files):
        t0 = time.perf_counter()
        if fs_arg:
            if nwm_file.find('https://') >= 0:
                _, bucket_key = convert_url2key(nwm_file,fs_type_arg)
            else:
                bucket_key = nwm_file
            file_obj   = fs_arg.open(bucket_key, mode='rb')
            nwm_file_sizes_MB.append(file_obj.details['size'])
        elif 'https://' in nwm_file:
            response = requests.get(nwm_file, timeout=10)

            if response.status_code == 200:
                file_obj = BytesIO(response.content)
            else:
                raise RuntimeError(f"{nwm_file} does not exist")
            nwm_file_sizes_MB.append(len(response.content) / B2MB)
        else:
            file_obj = nwm_file
            nwm_file_sizes_MB.append(os.path.getsize(nwm_file / B2MB))

        topen += time.perf_counter() - t0
        t0 = time.perf_counter()
        with xr.open_dataset(file_obj) as nwm_data:
            txrds += time.perf_counter() - t0
            t0 = time.perf_counter()
            data_allnwm = {}
            subset = nwm_data.sel(feature_id=nwm_cats)
            if "retrospective" in nwm_file:
                data_allnwm = dict(zip(subset['feature_id'].values,subset['q_lateral'].values))
                t = datetime.strftime(datetime.strptime(
                    nwm_file.split('/')[-1].split('.')[0],'%Y%m%d%H'),'%Y-%m-%d %H:%M:%S')
            else:
                # q_lateral is calculated by adding these three together
                subset['q_lateral'] = (subset['qSfcLatRunoff'] + subset['qBucket'] +
                                         subset['qBtmVertRunoff'])
                data_allnwm = dict(zip(subset['feature_id'].values,subset['q_lateral'].values))
                time_splt = subset.attrs["model_output_valid_time"].split("_")
                t = time_splt[0] + " " + time_splt[1]
            t_list.append(t)
        del nwm_data, subset
        tfill += time.perf_counter() - t0

        t0 = time.perf_counter()
        data_allngen = {}
        for ngen_nex, nwm_ids in mapping_arg.items():
            temp_array = []
            for nwm_id in nwm_ids:
                temp_array.append(data_allnwm[nwm_id])

            data_allngen[ngen_nex] = sum(temp_array)
        data_array = np.array(list(data_allngen.items()))

        data_list.append(data_array)
        tdata += time.perf_counter() - t0
        ttotal = topen + txrds + tfill + tdata
        if ii_verbose_arg:
            print(f'\nAverage time for:\nfs open file: {topen/(j+1):.2f} s\n', end=None,flush=True)
            print(f'xarray open dataset: {txrds/(j+1):.2f} s\nfill array: {tfill/(j+1):.2f} s\n',
                  end=None,flush=True)
            print(f'calculate catchment values: {tdata/(j+1):.2f} s\ntotal {ttotal/(j+1):.2f} s\n',
                  end=None,flush=True)
            print(f'percent complete {100*(j+1)/nfiles:.2f}', end=None,flush=True)
        report_usage()

    if ii_verbose_arg:
        print(f'Process #{id} completed data extraction, returning data to primary process',
              flush=True)
    return [data_list, t_list, nwm_file_sizes_MB]

def write_netcdf_chrt(storage_type: str, prefix: Path, data: np.ndarray, times: list, name: str):
    """
    Write channel routing data to a NetCDF file.

    Parameters:
        storage_type (str): s3 or local
        prefix (Path): filename prefix
        data (numpy.ndarray): 2D array with dimensions (nexus-id, qlateral).
        times (list): list representing time axis.
        name (str): string for the filename
    Returns:
        netcdf_cat_file_size (int): file size of output netcdf
    """
    if storage_type == 's3':
        s3_client = boto3.session.Session().client("s3")
        nc_filename = str(prefix) + "/" + name
    else:
        nc_filename = Path(prefix, name)

    time_coord = pd.to_datetime(times)
    feature_ids = data[0, :, 0]
    q_lateral = data[:, :, 1].astype(float)

    ds = xr.Dataset(
    {
        "q_lateral": (("time", "feature_id"), q_lateral)
    },
    coords={
        "time": time_coord,
        "feature_id": feature_ids
        }
    )

    if storage_type == 's3':
        bucket, key = convert_url2key(nc_filename,'s3')
        ds.to_netcdf(nc_filename)
        netcdf_cat_file_size = os.path.getsize(nc_filename) / B2MB
        print(f"Uploading netcdf forcings to S3: bucket={bucket}, key={key}")
        s3_client.upload_file(nc_filename, bucket, key)
        os.remove(nc_filename)
    else:
        ds.to_netcdf(nc_filename)
        print(f'netcdf has been written to {nc_filename}')
        netcdf_cat_file_size = os.path.getsize(nc_filename) / B2MB
    return netcdf_cat_file_size