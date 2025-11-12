from forcingprocessor.weights_hf2ds import hf2ds, multiprocess_hf2ds
from pathlib import Path
import os
import requests
import pyarrow.parquet as pq

HF_VERSION="v2.1.1"
test_dir = Path(__file__).parent
data_dir = (test_dir/'data').resolve()
os.makedirs(data_dir, exist_ok=True)
out_parq = os.path.join(data_dir,"out.parquet")
parq_name = "09_weights.parquet"
parq_path = os.path.join(data_dir,parq_name)

geopackage_name = "palisade.gpkg"
gpkg_path = os.path.join(data_dir,geopackage_name)
raster = "https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/CONUS/netcdf/FORCING/2018/201801010000.LDASIN_DOMAIN1"

def download_and_verify_parquet(url, filepath):
    """Download parquet file and verify it's valid"""
    try:
        print(f"Downloading {url} to {filepath}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Verify it's a valid parquet file
        pq.read_table(filepath)
        print(f"Successfully downloaded and verified parquet file: {filepath}")
        return True
    except Exception as e:
        print(f"Failed to download or verify parquet file: {e}")
        if os.path.exists(filepath):
            os.remove(filepath)
        return False

def test_parquet_v21():
    print(f"https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/{HF_VERSION}/nextgen/conus_forcing-weights/vpuid%3D09/part-0.parquet has moved!!!")
    # os.system(f"curl -o {parq_path} -L -O https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/{HF_VERSION}/nextgen/conus_forcing-weights/vpuid%3D09/part-0.parquet")
    # weights,_ = hf2ds([parq_path],raster,1)
    # assert len(weights) > 0

def test_gpkg_v21():
    url = f"https://ngen-datastream.s3.us-east-2.amazonaws.com/{geopackage_name}"
    if download_gpkg(url, gpkg_path):
        weights,_ = hf2ds([gpkg_path],raster,1)
        assert len(weights) > 0
    else:
        import pytest
        pytest.skip("Could not download GPKG file")

def test_gpkg_v22():
    weights,_ = hf2ds(["https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/vpu-09_subset.gpkg"],raster,1)
    assert len(weights) > 0

def test_multiple_parquet_v21():
    print(f"https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/{HF_VERSION}/nextgen/conus_forcing-weights/vpuid%3D09/part-0.parquet has moved!!!")
    # os.system(f"curl -o {parq_path} -L -O https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/{HF_VERSION}/nextgen/conus_forcing-weights/vpuid%3D09/part-0.parquet")
    # weights,_ = hf2ds([parq_path,parq_path],raster,1)
    # assert len(weights) > 0

def test_multiple_gpkg_v21():
    url = f"https://ngen-datastream.s3.us-east-2.amazonaws.com/{geopackage_name}"
    if download_gpkg(url, gpkg_path):
        weights,_ = hf2ds([gpkg_path,gpkg_path],raster,1)
        assert len(weights) > 0
    else:
        import pytest
        pytest.skip("Could not download GPKG file")

def test_multiple_gpkg_v22():
    weights,_ = hf2ds(["https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/vpu-09_subset.gpkg","https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/vpu-09_subset.gpkg"],raster,1)
    assert len(weights) > 0    

def test_multiple_multiprocess_parquet_v21():
    print(f"https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/{HF_VERSION}/nextgen/conus_forcing-weights/vpuid%3D09/part-0.parquet has moved!!!")
    # os.system(f"curl -o {parq_path} -L -O https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/{HF_VERSION}/nextgen/conus_forcing-weights/vpuid%3D09/part-0.parquet")
    # weights,_ = multiprocess_hf2ds([parq_path,parq_path],raster,2)
    # assert len(weights) > 0

def test_multiple_multiprocess_gpkg_v21():
    url = f"https://ngen-datastream.s3.us-east-2.amazonaws.com/{geopackage_name}"
    if download_gpkg(url, gpkg_path):
        weights,_ = multiprocess_hf2ds([gpkg_path,gpkg_path],raster,2)
        assert len(weights) > 0
    else:
        import pytest
        pytest.skip("Could not download GPKG file")

def test_multiple_multiprocess_gpkg_v22():
    weights,_ = multiprocess_hf2ds(["https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/vpu-09_subset.gpkg","https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/vpu-09_subset.gpkg"],raster,2)
    assert len(weights) > 0       

