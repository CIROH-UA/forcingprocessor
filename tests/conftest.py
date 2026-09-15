import pytest, os, shutil
from pathlib import Path
from forcingprocessor.utils import vpus

test_dir = Path(__file__).parent
data_dir = (test_dir / "data").resolve()
forcings_dir = (data_dir / "forcings").resolve()
metadata_dir = (data_dir / "metadata").resolve()
geopackage_name = "vpu-09_subset.gpkg"
weight_files = [
    f"https://ciroh-community-ngen-datastream.s3.amazonaws.com/resources/v2.2_hydrofabric/weights/nextgen_VPU_{x}_weights.json"
    for x in vpus
]
local_weight_files = [
    str((data_dir / f"nextgen_VPU_{x}_weights.json").resolve()) for x in vpus
]

# Fixtures that hit the network (public URLs, no credentials needed)
NETWORK_FIXTURES = {"download_gpkg", "download_weight_file", "download_weights"}
# Fixtures that require AWS credentials (read/write a private S3 bucket)
AWS_CREDS_FIXTURES = {"clean_s3_test", "clean_s3_nrds_test"}


def pytest_addoption(parser):
    parser.addoption(
        "--no-skip-creds",
        action="store_true",
        default=False,
        help="Do not auto-skip aws_creds tests even if credentials look absent.",
    )


def _has_aws_creds():
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    if os.path.exists(os.path.expanduser("~/.aws/credentials")):
        return True
    return bool(os.environ.get("AWS_PROFILE") or os.environ.get("AWS_SESSION_TOKEN"))


def pytest_collection_modifyitems(config, items):
    """
    Auto-apply the `network` and `aws_creds` markers to any test that
    requests one of the fixtures above, so contributors don't have to
    remember to hand-tag every new test that happens to use them.
    Tests that hit the network without going through a fixture still
    need an explicit @pytest.mark.network decorator.

    Also auto-skips `aws_creds` tests when no AWS credentials are
    detected in the environment, unless --no-skip-creds is passed.
    """
    creds_present = _has_aws_creds()
    skip_creds = pytest.mark.skip(reason="AWS credentials not available")

    for item in items:
        fixtures_used = set(getattr(item, "fixturenames", []))
        if fixtures_used & NETWORK_FIXTURES:
            item.add_marker(pytest.mark.network)
        if fixtures_used & AWS_CREDS_FIXTURES:
            item.add_marker(pytest.mark.aws_creds)

        if (
            "aws_creds" in item.keywords
            and not creds_present
            and not config.getoption("--no-skip-creds")
        ):
            item.add_marker(skip_creds)
            

@pytest.fixture(scope="session")
def clean_s3_nrds_test():
    os.system(
        "aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest/nrds_fp_test --recursive"
    )
    yield
    os.system(
        "aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest/nrds_fp_test --recursive"
    )


@pytest.fixture(scope="session")
def clean_s3_test():
    os.system(
        "aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest --recursive"
    )
    yield
    os.system(
        "aws s3 rm s3://ciroh-community-ngen-datastream/test/cicd/forcingprocessor/pytest --recursive"
    )


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
    os.system(
        f"curl -o {os.path.join(data_dir, geopackage_name)} -L -O https://datastream-resources.s3.us-east-1.amazonaws.com/VPU_09/config/nextgen_VPU_09.gpkg"
    )
    yield


@pytest.fixture(scope="session")
def download_weight_file():
    weights_name = "nextgen_VPU_09_weights.json"
    local_path = os.path.join(data_dir, weights_name)
    if not os.path.exists(local_path):
        os.system(
            f"curl -o {local_path} -L -O https://ciroh-community-ngen-datastream.s3.amazonaws.com/resources/v2.2_hydrofabric/weights/nextgen_VPU_09_weights.json"
        )
    yield


@pytest.fixture(scope="session")
def download_weights():
    for j, wf in enumerate(weight_files):
        local_file = local_weight_files[j]
        if not os.path.exists(local_file):
            os.system(f"wget {wf} -P {data_dir}")
    yield
