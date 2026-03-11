"""
Unit tests for restart utility functions.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from forcingprocessor.troute_restart_tools import (
    average_nwm_variables,
    average_rtlink_variables,
    create_restart,
    quadratic_formula,
    solve_depth_geom,
)


# ---------------------------------------------------------------------------
# minimum viable examples
# ---------------------------------------------------------------------------

simple_nwm_ds = xr.Dataset(
    {
        "streamflow": ("feature_id", [10.0, 20.0, 30.0, 40.0]),
        "velocity": ("feature_id", [1.0, 2.0, 3.0, 4.0]),
    },
    coords={
        "feature_id": [101, 102, 103, 104],
        "time": [np.datetime64("2024-01-15T12:00:00.000000000")],
    },
)

simple_routelink_ds = xr.Dataset(
    {
        "link": ("feature_id", [101, 102, 103, 104]),
        "TopWdth": ("feature_id", [10.0, 12.0, 14.0, 16.0]),
        "BtmWdth": ("feature_id", [5.0, 6.0, 7.0, 8.0]),
        "ChSlp": ("feature_id", [0.5, 0.5, 0.5, 0.5]),
    },
    coords={},
)

simple_crosswalk_ds = xr.Dataset(coords={"link": [1, 2]})

simple_cat_map = {
    "cat-1": [101.0, 102.0],
    "cat-2": [103.0, 104.0],
}


# ---------------------------------------------------------------------------
# unit tests
# ---------------------------------------------------------------------------


def test_averages_streamflow_and_velocity():
    # cat 1 -> features 101 (sf=10, v=1) and 102 (sf=20, v=2) => mean sf=15, v=1.5
    # cat 2 -> feature  103 (sf=30, v=3)                       => mean sf=30, v=3.0
    nwm_ids = np.array([101.0, 102.0, 103.0])
    cat_ids = np.array([1, 1, 2])
    agg, mapping = average_nwm_variables(nwm_ids, cat_ids, simple_nwm_ds)

    assert agg.loc[1, "streamflow"] == pytest.approx(15.0)  # test averaging
    assert agg.loc[1, "velocity"] == pytest.approx(1.5)
    assert agg.loc[2, "streamflow"] == pytest.approx(30.0)
    assert agg.loc[2, "velocity"] == pytest.approx(3.0)

    assert len(mapping) == 3  # test mapping
    assert set(mapping.columns) == {"feature_id", "cat_id"}

    assert 3 not in agg.index  # test subsetting
    assert len(agg) == 2


def test_averages_routelink():
    nwm_ids = np.array([101.0, 102.0, 103.0, 104.0])
    mapping = pd.DataFrame(
        {"feature_id": [101.0, 102.0, 103.0, 104.0], "cat_id": [1, 1, 2, 2]}
    )
    agg = average_rtlink_variables(nwm_ids, mapping, simple_routelink_ds)

    assert agg.loc[1, "TopWdth"] == pytest.approx(11.0)  # test averaging
    assert agg.loc[2, "TopWdth"] == pytest.approx(15.0)
    assert agg.loc[1, "BtmWdth"] == pytest.approx(5.5)
    assert agg.loc[2, "BtmWdth"] == pytest.approx(7.5)
    assert agg.loc[1, "ChSlp"] == pytest.approx(0.5)
    assert agg.loc[2, "ChSlp"] == pytest.approx(0.5)

    assert len(agg) == 2  # test layout


def test_quadratic_formula():
    # Two equations: [x^2+2x-3, x^2-4] -> roots [1, 2]
    b = np.array([2.0, 0.0])
    c = np.array([-3.0, -4.0])
    result = quadratic_formula(b, c)
    np.testing.assert_allclose(result, [1.0, 2.0])


def test_solve_depth_geom():
    sf = np.array([0.0, 5.0, 2.0, 8.0, 1000.0, np.nan])
    v = np.array([1.0, 0.0, 1.0, 1.0, 1.0, 1.0])
    tw = np.array([10.0, 10.0, 10.0, 10.0, 10.0, 10.0])
    bw = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0])
    cs = np.array([0.5, 0.5, 0.5, 0.5, 0.5, 0.5])
    depths = solve_depth_geom(sf, v, tw, bw, cs)

    assert depths.shape == (6,)
    assert depths[0] == pytest.approx(0.0)
    assert depths[1] == pytest.approx(0.0)
    assert depths[2] == pytest.approx(0.35078106)
    assert depths[3] == pytest.approx(1.10849528)
    assert depths[4] == pytest.approx(34.27083333)
    assert depths[5] == pytest.approx(0.0)


def test_restart():
    result = create_restart(
        simple_cat_map, simple_crosswalk_ds, simple_nwm_ds, simple_routelink_ds
    )

    n_links = len(simple_crosswalk_ds["link"])
    assert result["hlink"].shape == (n_links,)
    assert result["qlink1"].shape == (n_links,)
    assert result["qlink2"].shape == (n_links,)

    assert result.attrs["Restart_Time"] == "2024-01-15_12:00:00"

    assert result["hlink"].values[0] == pytest.approx(1.25)
    assert result["hlink"].values[1] == pytest.approx(1.04315438)
    assert (
        result["qlink1"].values[0] == result["qlink2"].values[0] == pytest.approx(15.0)
    )
    assert (
        result["qlink1"].values[1] == result["qlink2"].values[1] == pytest.approx(35.0)
    )

