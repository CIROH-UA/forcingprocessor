from forcingprocessor.utils import normalize_vpu_id


def test_normalize_vpu_id():
    assert normalize_vpu_id("03W") == "VPU_03W"
    assert normalize_vpu_id("vpu_03w") == "VPU_03W"
    assert normalize_vpu_id("nextgen_VPU_10L.gpkg") == "VPU_10L"
