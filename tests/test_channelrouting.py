"""Tests for channel routing-based t-route input generation."""

import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import re
import pytest
from forcingprocessor.processor import prep_ngen_data
from forcingprocessor.nwm_filenames_generator import generate_nwmfiles

HF_VERSION="v2.2"
date = datetime.now(timezone.utc)
date = date.strftime('%Y%m%d')
HOURMINUTE  = '0000'
TODAY_START = date + HOURMINUTE
yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
yesterday = yesterday.strftime('%Y%m%d')
test_dir = Path(__file__).parent
data_dir = (test_dir/'data').resolve()
forcings_dir = (data_dir/'forcings').resolve()
pwd      = Path.cwd()
if os.path.exists(data_dir):
    os.system(f"rm -rf {data_dir}")
os.system(f"mkdir {data_dir}")
FILENAMELIST = str((pwd/"filenamelist.txt").resolve())
RETRO_FILENAMELIST = str((pwd/"retro_filenamelist.txt").resolve())

conf = {
    "forcing"  : {
        "nwm_file"   : FILENAMELIST,
        "gpkg_file"  : [f"{pwd}/docs/examples/routing-only_example/cat-1555522_subset.gpkg"],
        "map_file"   : "s3://ciroh-community-ngen-datastream/mappings/nwm_to_ngen_map.json"
    },

    "storage":{
        "storage_type"      : "local",
        "output_path"       : str(data_dir),
        "output_file_type"  : ["netcdf"]
    },

    "run" : {
        "verbose"       : False,
        "collect_stats" : False,
        "nprocs"         : 1
    }
    }

nwmurl_conf = {
        "forcing_type" : "operational_archive",
        "start_date"   : "",
        "end_date"     : "",
        "runinput"     : 1,
        "varinput"     : 1,
        "geoinput"     : 1,
        "meminput"     : 1,
        "urlbaseinput" : 7,
        "fcst_cycle"   : [0],
        "lead_time"    : [1]
    }

nwmurl_conf_retro = {
        "forcing_type" : "retrospective",
        "start_date"   : "201801010000",
        "end_date"     : "201801010000",
        "urlbaseinput" : 1,
        "selected_object_type" : [2],
        "selected_var_types"   : [1],
        "write_to_file" : True
    }

@pytest.fixture
def clean_dir(autouse=True):
    if os.path.exists(forcings_dir):
        os.system(f'rm -rf {str(forcings_dir)}')

def test_nomads_prod(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 1
    generate_nwmfiles(nwmurl_conf)
    conf['run']['collect_stats'] = True # test metadata generation once
    prep_ngen_data(conf)
    conf['run']['collect_stats'] = False
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_nwm_google_apis(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 3
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_google_cloud_storage(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = "202407100100"
    nwmurl_conf['end_date']   = "202407100100"
    nwmurl_conf["urlbaseinput"] = 4
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_gs(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 5
    generate_nwmfiles(nwmurl_conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    prep_ngen_data(conf)
    assert assert_file.exists()
    os.remove(assert_file)

def test_gcs(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = "202407100100"
    nwmurl_conf['end_date']   = "202407100100"
    nwmurl_conf["urlbaseinput"] = 6
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_noaa_nwm_pds_https(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 7
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_noaa_nwm_pds_https_short_range(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 7
    nwmurl_conf["runinput"] = 1
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_noaa_nwm_pds_https_medium_range(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 7
    nwmurl_conf["runinput"] = 2
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(
        data_dir/"forcings/ngen.t00z.medium_range_mem1.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_noaa_nwm_pds_https_analysis_assim(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["urlbaseinput"] = 7
    nwmurl_conf["runinput"] = 5
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(
        data_dir/"forcings/ngen.t00z.analysis_assim.channel_routing.tm01_tm01.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_noaa_nwm_pds_https_analysis_assim_extend(
        download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = yesterday + HOURMINUTE
    nwmurl_conf['end_date']   = yesterday + HOURMINUTE
    nwmurl_conf["urlbaseinput"] = 7
    nwmurl_conf["runinput"] = 6
    nwmurl_conf["fcst_cycle"] = [16]
    generate_nwmfiles(nwmurl_conf)
    try:
        prep_ngen_data(conf)
    except Exception as e:
        pattern = (
            r"https://noaa-nwm-pds\.s3\.amazonaws\.com/nwm\.\d{8}/" +
            r"forcing_analysis_assim_extend/nwm\.t16z\.analysis_assim_extend" +
            r"\.channel_routing\.tm01\.conus\.nc does not exist")
        if re.fullmatch(pattern, str(e)):
            pytest.skip(f"Upstream datafile missing: {e}")
        else:
            raise
    assert_file=(
        data_dir/"forcings/ngen.t16z.analysis_assim_extend.channel_routing.tm01_tm01.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_noaa_nwm_pds_s3(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["runinput"] = 1
    nwmurl_conf["urlbaseinput"] = 8
    nwmurl_conf["fcst_cycle"] = [0]
    generate_nwmfiles(nwmurl_conf)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_retro_2_1_https(download_weight_file,clean_forcings_metadata_dirs):
    conf['forcing']['nwm_file'] = RETRO_FILENAMELIST
    nwmurl_conf_retro["urlbaseinput"] = 1
    generate_nwmfiles(nwmurl_conf_retro)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/qlaterals.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_retro_2_1_s3(download_weight_file,clean_forcings_metadata_dirs):
    conf['forcing']['nwm_file'] = RETRO_FILENAMELIST
    print(conf['forcing']['nwm_file'], flush=True)
    nwmurl_conf_retro["urlbaseinput"] = 2
    generate_nwmfiles(nwmurl_conf_retro)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/qlaterals.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_retro_3_0(download_weight_file,clean_forcings_metadata_dirs):
    conf['forcing']['nwm_file'] = RETRO_FILENAMELIST
    nwmurl_conf_retro["urlbaseinput"] = 4
    generate_nwmfiles(nwmurl_conf_retro)
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/qlaterals.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_s3_output(download_weight_file,clean_forcings_metadata_dirs,clean_s3_test):
    test_bucket = "ciroh-community-ngen-datastream"
    if 'plot' in conf.keys():
        conf.pop('plot')
    conf['forcing']['nwm_file'] = RETRO_FILENAMELIST
    conf['storage']['output_path'] = f's3://{test_bucket}/test/cicd/forcingprocessor/pytest'
    conf['storage']['output_file_type'] = ["netcdf"]
    nwmurl_conf_retro["urlbaseinput"] = 4
    generate_nwmfiles(nwmurl_conf_retro)
    prep_ngen_data(conf)
    conf['storage']['output_path'] = str(data_dir)

def test_csv_output_type(download_weight_file,clean_forcings_metadata_dirs):
    nwmurl_conf['start_date'] = TODAY_START
    nwmurl_conf['end_date']   = TODAY_START
    nwmurl_conf["runinput"] = 1
    nwmurl_conf["urlbaseinput"] = 8
    nwmurl_conf["fcst_cycle"] = [0]
    generate_nwmfiles(nwmurl_conf)
    conf['storage']['output_file_type'] = ["csv"]
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/nex-1555523.csv").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_parquet_output_type(download_weight_file,clean_forcings_metadata_dirs):
    generate_nwmfiles(nwmurl_conf)
    conf['storage']['output_file_type'] = ["parquet"]
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/nex-1555523.parquet").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_tar_output_type(download_weight_file,clean_forcings_metadata_dirs):
    generate_nwmfiles(nwmurl_conf)
    conf['storage']['output_file_type'] = ["tar"]
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/1_forcings.tar.gz").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

def test_netcdf_output_type(download_weight_file,clean_forcings_metadata_dirs):
    generate_nwmfiles(nwmurl_conf)
    conf['storage']['output_file_type'] = ["netcdf"]
    prep_ngen_data(conf)
    assert_file=(data_dir/"forcings/ngen.t00z.short_range.channel_routing.f001_f001.nc").resolve()
    assert assert_file.exists()
    os.remove(assert_file)

