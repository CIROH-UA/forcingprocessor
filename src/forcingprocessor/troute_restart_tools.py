"""
Tools to extract and write streamflow and depth values into a restart format ingestible by
t-route. Translates between NWM and NGEN IDs!
"""

import xarray as xr
import numpy as np
import pandas as pd


def average_nwm_variables(
    nwm_ids_flat: np.ndarray, cat_ids_flat: np.ndarray, nwm_ds: xr.Dataset
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Vectorized averaging calculations for NWM data

    Parameters:
    - nwm_ids_flat: array of NWM ids (np.ndarray)
    - cat_ids_flat: array of NextGen cat-ids (np.ndarray)
    - nwm_ds: NWM analysis/assimilation data (xr.Dataset)

    Returns:
    - nwm_agg: averaged NWM data (pd.DataFrame)
    - mapping_df: DataFrame version of flat maps (pd.DataFrame)
    """

    # --- NWM dataset: streamflow and velocity ---
    # Filter nwm_ds to only feature_ids we care about
    valid_mask = np.isin(nwm_ds["feature_id"].values, nwm_ids_flat)
    nwm_sub = nwm_ds.isel(feature_id=valid_mask)

    # Build a df with feature_id -> cat_id, then merge with nwm values
    mapping_df = pd.DataFrame({"feature_id": nwm_ids_flat, "cat_id": cat_ids_flat})

    nwm_df = pd.DataFrame(
        {
            "feature_id": nwm_sub["feature_id"].values,
            "streamflow": nwm_sub["streamflow"].values,
            "velocity": nwm_sub["velocity"].values,
        }
    )

    nwm_df = nwm_df.merge(mapping_df, on="feature_id", how="left")
    nwm_agg = nwm_df.groupby("cat_id")[["streamflow", "velocity"]].mean()

    return nwm_agg, mapping_df


def average_rtlink_variables(
    nwm_ids_flat: np.ndarray, mapping_df: pd.DataFrame, routelink_ds: xr.Dataset
) -> pd.DataFrame:
    """
    Vectorized averaging calculations for RouteLink data

    Parameters
    - nwm_ids_flat: array of NWM ids (np.ndarray)
    - mapping_df: dataframe of flat map (pd.DataFrame)
    - routelink_ds: NWM RouteLink data (xr.Dataset)

    Returns:
    - rl_agg: averaged NWM RouteLink channel geometry data (pd.DataFrame)
    """

    # --- Routelink dataset: TopWdth, BtmWdth, ChSlp ---
    valid_mask_rl = np.isin(routelink_ds["link"].values, nwm_ids_flat)
    rl_sub = routelink_ds.isel(feature_id=valid_mask_rl)

    rl_df = pd.DataFrame(
        {
            "feature_id": rl_sub["link"].values,
            "TopWdth": rl_sub["TopWdth"].values,
            "BtmWdth": rl_sub["BtmWdth"].values,
            "ChSlp": rl_sub["ChSlp"].values,
        }
    )

    rl_df = rl_df.merge(mapping_df, on="feature_id", how="left")
    rl_agg = rl_df.groupby("cat_id")[["TopWdth", "BtmWdth", "ChSlp"]].mean()

    return rl_agg


def quadratic_formula(b_coeff: np.ndarray, c_coeff: np.ndarray) -> np.ndarray:
    """
    Vectorized quadratic formula solver (assumes no a coefficient). Only returns positive root

    Parameters:
    - b_coeff: np.ndarray
    - c_coeff: np.ndarray

    Returns:
    - h_positive: positive root (np.ndarray)
    """
    discriminant = b_coeff**2 - 4 * c_coeff
    h_positive = (-b_coeff + np.sqrt(discriminant)) / 2

    return h_positive


def solve_depth_geom(
    streamflow: np.ndarray,
    velocity: np.ndarray,
    tw: np.ndarray,
    bw: np.ndarray,
    cs: np.ndarray,
) -> np.ndarray:
    """
    Solves for depth h using CHRTOUT file variables and channel geometry variables.

    Parameters:
    - streamflow: Streamflow from CHRTOUT file. (m^3/s)
    - velocity: Velocity from CHRTOUT file. (m/s)
    - tw: Top width of the main channel. (m)
    - bw: Bottom width of the main channel. (m)
    - cs: Channel slope (dimensionless).

    Returns:
    - h: Initial depth that achieves the target flow rate, or NaN if no solution is found.
    """

    area = streamflow / velocity  # cross-sectional area of initial flow
    area = np.where(np.isnan(area), 0, area)  # set NaN areas to 0
    area = np.where(np.isinf(area), 0, area)  # set infinite areas to 0

    db = (cs * (tw - bw)) / 2  # bankfull depth
    area_bankfull = (tw + bw) / 2 * db  # cross-sectional area at bankfull conditions
    # assume trapezoidal main channel

    depths = np.zeros_like(area)  # initialize depths array with 0 values

    above_bankfull = area >= area_bankfull
    area_flood = area[above_bankfull] - area_bankfull[above_bankfull]
    df = area_flood / (tw[above_bankfull] * 3)
    depths[above_bankfull] = db[above_bankfull] + df

    # Below bankfull - solve quadratic formula directly (vectorized)
    below_bankfull = ~above_bankfull & (area > 0)

    # Quadratic: h^2 + cs*bw*h - cs*area = 0
    # Using formula: h = (-b + sqrt(b^2 + 4*c)) / 2, where a=1
    h_positive = quadratic_formula(
        cs[below_bankfull] * bw[below_bankfull],
        -cs[below_bankfull] * area[below_bankfull],
    )
    depths[below_bankfull] = h_positive

    return depths


def create_restart(
    cat_map: dict,
    crosswalk_ds: xr.Dataset,
    nwm_ds: xr.Dataset,
    routelink_ds: xr.Dataset,
) -> xr.Dataset:
    """
    Creates t-route restart file.

    Parameters:
    - cat_map: NGEN to NWM catchment json file (dict)
    - crosswalk_ds: "crosswalk" NetCDF file that has all the
    NextGen catchments in the order that the restart file will have them in (xr.Dataset)
    - nwm_ds: NWM analysis/assimilation NetCDF (xr.Dataset)
    - routelink_ds: NWM RouteLink channel geometry NetCDF (xr.Dataset)

    Returns:
    - restart: t-route ingestible restart file (xr.Dataset)
    """

    nwm_ids_flat = []
    cat_ids_flat = []
    for cat_id, nwm_ids in cat_map.items():
        for nwm_id in nwm_ids:
            nwm_ids_flat.append(nwm_id)
            cat_ids_flat.append(cat_id)

    nwm_ids_flat = np.array(nwm_ids_flat, dtype=float)
    cat_ids_flat = np.array(cat_ids_flat)

    nwm_agg, mapping_df = average_nwm_variables(nwm_ids_flat, cat_ids_flat, nwm_ds)
    rl_agg = average_rtlink_variables(nwm_ids_flat, mapping_df, routelink_ds)
    result_df = pd.DataFrame({"cat_id": crosswalk_ds["link"].values})
    result_df = result_df.join(nwm_agg, on="cat_id").join(rl_agg, on="cat_id").fillna(0)

    # depth calculation
    depths = solve_depth_geom(
        streamflow=np.array(result_df["streamflow"].values),
        velocity=np.array(result_df["velocity"].values),
        tw=np.array(result_df["TopWdth"].values),
        bw=np.array(result_df["BtmWdth"].values),
        cs=np.array(result_df["ChSlp"].values),
    )
    result_df["depth"] = depths

    # create netcdf
    restart = xr.Dataset(
        data_vars={
            "hlink": (["links"], result_df["depth"].values),
            "qlink1": (["links"], result_df["streamflow"].values),
            "qlink2": (["links"], result_df["streamflow"].values),
        },
        coords={"links": range(len(result_df))},
        attrs={
            "Restart_Time": pd.Timestamp(nwm_ds["time"].values[0]).strftime(
                "%Y-%m-%d_%H:%M:%S"
            )
        },
    )

    return restart
