import numpy as np
import pandas as pd
import pytest
from forcingprocessor import processor
from forcingprocessor.processor import (
    distribute_work,
    load_balance,
    calculate_vpu_precip_stats,
)
from forcingprocessor.utils import ngen_variables

PRECIP_IDX = ngen_variables.index("precip_rate")


@pytest.fixture(autouse=True)
def _set_ii_verbose_global():
    """
    load_balance() reads the module-level global `ii_verbose`, which is
    normally only set as a side effect of calling prep_ngen_data() first
    (processor.py line ~1050: `global ii_verbose; ii_verbose = ...`).
    Since these tests call load_balance() directly without going through
    prep_ngen_data, we set the global explicitly here so the function
    doesn't raise NameError. This is a real coupling worth cleaning up
    separately (load_balance isn't actually side-effect-free).
    """
    processor.ii_verbose = False
    yield


# ---------------------------------------------------------------------------
# distribute_work
# ---------------------------------------------------------------------------

def test_distribute_work_even_split():
    result = distribute_work(list(range(10)), 5)
    assert result == [2, 2, 2, 2, 2]


def test_distribute_work_uneven_split_round_robin():
    result = distribute_work(list(range(7)), 3)
    assert result == [3, 2, 2]
    assert sum(result) == 7


def test_distribute_work_more_procs_than_items():
    result = distribute_work(list(range(2)), 5)
    assert result == [1, 1, 0, 0, 0]
    assert sum(result) == 2


def test_distribute_work_single_proc():
    result = distribute_work(list(range(9)), 1)
    assert result == [9]


def test_distribute_work_empty_items():
    result = distribute_work([], 4)
    assert result == [0, 0, 0, 0]


# ---------------------------------------------------------------------------
# load_balance
# ---------------------------------------------------------------------------

def test_load_balance_preserves_total_item_count():
    items_per_proc = [10, 0, 0]
    result = load_balance(items_per_proc, launch_delay=1, single_ex=1, exec_count=1)
    assert sum(result) == 10


def test_load_balance_returns_list_length_le_input():
    items_per_proc = [5, 5]
    result = load_balance(items_per_proc, launch_delay=0.5, single_ex=1, exec_count=1)
    assert len(result) <= len(items_per_proc)


@pytest.mark.xfail(
    reason=(
        "Discovered bug: load_balance's loop-exit condition is inverted for "
        "the all-zero case (`if nonzero_count > 0: break` never breaks when "
        "everything is 0), so it shuffles phantom work between procs and "
        "produces a negative item count that gets silently truncated. "
        "Returned list currently sums to 1 instead of 0. Filing separately, "
        "not fixing here since it's out of scope for the test-tagging PR (#115)."
    ),
    strict=True,
)
def test_load_balance_all_zero_items_noop():
    items_per_proc = [0, 0, 0]
    result = load_balance(items_per_proc, launch_delay=1, single_ex=1, exec_count=1)
    assert sum(result) == 0

# ---------------------------------------------------------------------------
# calculate_vpu_precip_stats
# ---------------------------------------------------------------------------

def _make_data_array(n_time, n_vars, precip_values_by_catchment):
    n_catch = len(precip_values_by_catchment)
    arr = np.zeros((n_time, n_vars, n_catch))
    for c_idx, val in enumerate(precip_values_by_catchment):
        arr[:, PRECIP_IDX, c_idx] = val
    return arr


def test_calculate_vpu_precip_stats_basic_values():
    catchment_ids = ["cat-1", "cat-2", "cat-3"]
    precip_values = [0.0, 2.0, 4.0]
    data_array = _make_data_array(3, len(ngen_variables), precip_values)
    jcatchment_dict = {"VPU_09": catchment_ids}

    df = calculate_vpu_precip_stats(data_array, catchment_ids, jcatchment_dict)

    assert list(df["vpu_id"]) == ["VPU_09"]
    row = df.iloc[0]
    assert row["precip_min"] == 0.0
    assert row["precip_max"] == 4.0
    assert row["precip_mean"] == pytest.approx(2.0)
    assert row["precip_sum"] == pytest.approx(18.0)
    assert row["precip_nonzero_fraction"] == pytest.approx(6 / 9)


def test_calculate_vpu_precip_stats_multiple_vpus():
    catchment_ids = ["cat-1", "cat-2", "cat-3", "cat-4"]
    precip_values = [1.0, 1.0, 5.0, 5.0]
    data_array = _make_data_array(1, len(ngen_variables), precip_values)
    jcatchment_dict = {"VPU_01": ["cat-1", "cat-2"], "VPU_02": ["cat-3", "cat-4"]}

    df = calculate_vpu_precip_stats(data_array, catchment_ids, jcatchment_dict)

    assert set(df["vpu_id"]) == {"VPU_01", "VPU_02"}
    vpu1 = df.loc[df["vpu_id"] == "VPU_01"].iloc[0]
    vpu2 = df.loc[df["vpu_id"] == "VPU_02"].iloc[0]
    assert vpu1["precip_mean"] == pytest.approx(1.0)
    assert vpu2["precip_mean"] == pytest.approx(5.0)


def test_calculate_vpu_precip_stats_skips_vpu_with_no_matching_catchments():
    catchment_ids = ["cat-1", "cat-2"]
    precip_values = [3.0, 3.0]
    data_array = _make_data_array(1, len(ngen_variables), precip_values)
    jcatchment_dict = {"VPU_99": ["cat-does-not-exist"]}

    df = calculate_vpu_precip_stats(data_array, catchment_ids, jcatchment_dict)

    assert df.empty or "VPU_99" not in set(df["vpu_id"])


def test_calculate_vpu_precip_stats_all_zero_precip_nonzero_fraction():
    catchment_ids = ["cat-1", "cat-2"]
    precip_values = [0.0, 0.0]
    data_array = _make_data_array(2, len(ngen_variables), precip_values)
    jcatchment_dict = {"VPU_09": catchment_ids}

    df = calculate_vpu_precip_stats(data_array, catchment_ids, jcatchment_dict)

    row = df.iloc[0]
    assert row["precip_nonzero_fraction"] == 0.0
    assert row["precip_sum"] == 0.0