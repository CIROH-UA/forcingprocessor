import pytest, os, shutil
from pathlib import Path
from forcingprocessor.utils import vpus

test_dir = Path(__file__).parent
data_dir = (test_dir/'data').resolve()
forcings_dir = (data_dir/'forcings').resolve()
metadata_dir = (data_dir/'metadata').resolve()
geopackage_name = "vpu-09_subset.gpkg"
weight_files = [f"https://ciroh-community-ngen-datastream.s3.amazonaws.com/resources/v2.2_hydrofabric/weights/nextgen_VPU_{x}_weights.json" for x in vpus]
local_weight_files = [str((data_dir/f"nextgen_VPU_{x}_weights.json").resolve()) for x in vpus]

@pytest.fixture(scope="session")
def clean_s3_nrds_test():
    os.system("aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest/nrds_fp_test --recursive")
    yield
    os.system("aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest/nrds_fp_test --recursive")

@pytest.fixture(scope="session")
def clean_s3_test():
    os.system("aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest --recursive") 
    yield
    os.system("aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest --recursive")    

@pytest.fixture(scope="session")
def clean_data_dir():
    yield
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)           

@pytest.fixture()
def clean_forcings_metadata_dirs():
    if os.path.exists(forcings_dir):
        shutil.rmtree(forcings_dir)   
    if os.path.exists(metadata_dir):
        shutil.rmtree(metadata_dir)          
    yield 
    if os.path.exists(forcings_dir):
        shutil.rmtree(forcings_dir)  
    if os.path.exists(metadata_dir):
        shutil.rmtree(metadata_dir)           

@pytest.fixture(scope="session")
def download_gpkg():
    os.system(f"curl -o {os.path.join(data_dir,geopackage_name)} -L -O https://datastream-resources.s3.us-east-1.amazonaws.com/VPU_09/config/nextgen_VPU_09.gpkg")
    yield


@pytest.fixture(scope="session")
def download_weight_file():
    weights_name = "nextgen_VPU_09_weights.json"
    local_path = os.path.join(data_dir,weights_name)
    if not os.path.exists(local_path):
        os.system(f"curl -o {local_path} -L -O https://ciroh-community-ngen-datastream.s3.amazonaws.com/resources/v2.2_hydrofabric/weights/nextgen_VPU_09_weights.json")
    yield

@pytest.fixture(scope="session")
def download_weights():
    for j, wf in enumerate(weight_files):
        local_file = local_weight_files[j]
        if not os.path.exists(local_file):
            os.system(f"wget {wf} -P {data_dir}")    
    yield
