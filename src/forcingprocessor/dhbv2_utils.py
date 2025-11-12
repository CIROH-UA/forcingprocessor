"""Functions for dHBV2 model forcings processing."""

import warnings
import concurrent.futures as cf

import numpy as np
import pandas as pd
import geopandas as gpd

SOLAR_CONSTANT = 0.0820
TMP1 = (24.0 * 60.0) / np.pi

def add_pet_to_dataset(dataset: np.ndarray, t_ax: list, catchments: list,
                       cat_lats: dict) -> np.ndarray:
    '''
    Add calculated PET to dataset.

    Parameters:
        dataset : np.ndarray
            shape (num_timesteps, num_vars, num_catchments) where vars are
            precip and temperature
        t_ax : list
            length num_timesteps
        catchments : list
            length num_catchments
        cat_lats : dict
            {catchment_id: latitude}

    Returns:
        np.ndarray : shape (num_timesteps, num_vars, num_catchments) where vars
            are precip, temperature, and PET
    '''

    def hargreaves(tmin: np.ndarray, tmax: np.ndarray, tmean: np.ndarray,
                   lat: list, date: pd.Timestamp) -> np.ndarray:
        """
        Compute PET using Hargreaves equation.

        Parameters:
        tmax : np.ndarray
            shape (num_catchments, )
        tmin : np.ndarray
            shape (num_catchments, )
        tmean : np.ndarray
            shape (num_catchments, )
        lat : list
            length num_catchments
        date : pd.Timestamp

        Output:
        np.ndarray : shape (num_catchments, ), contains PET values
        """
        #calculate the day of year
        dfdate = date
        tempday = np.array(dfdate.timetuple().tm_yday)
        day_of_year = np.tile(tempday.reshape(-1, 1), [1, tmin.shape[-1]])

        # Loop to reduce memory usage
        pet = np.zeros(tmin.shape, dtype=np.float32) * np.nan

        temp_range = tmax - tmin
        temp_range[temp_range < 0] = 0

        latitude = np.deg2rad(lat)

        sol_dec = 0.409 * np.sin(((2.0 * np.pi / 365.0) * day_of_year - 1.39))
        sha = np.arccos(np.clip(-np.tan(latitude) * np.tan(sol_dec), -1, 1))
        ird = 1 + (0.033 * np.cos((2.0 * np.pi / 365.0) * day_of_year))
        tmp2 = sha * np.sin(latitude) * np.sin(sol_dec)
        tmp3 = np.cos(latitude) * np.cos(sol_dec) * np.sin(sha)
        et_rad = TMP1 * SOLAR_CONSTANT * ird * (tmp2 + tmp3)
        et_rad = et_rad.reshape(-1)
        pet = 0.0023 * (tmean + 17.8) * temp_range ** 0.5 * 0.408 * et_rad
        pet[pet < 0] = 0
        return pet

    # read 24 hour chunks at a time to calculate temperature stats
    # if a 24 hr chunk not available, then stats computed for whatever length of timestep is there
    num_ts, _, num_cats = dataset.shape

    day_chunk_start_idx = 0
    pet_array = np.empty((num_cats, num_ts))

    while day_chunk_start_idx <= num_ts - 1:
        ts_start = pd.to_datetime(t_ax[day_chunk_start_idx])
        if day_chunk_start_idx + 23 <= num_ts - 1:
            ts_diff = 24
        else: # in case there isn't a full day left in the forcings file
            ts_diff = num_ts-day_chunk_start_idx
        #day_chunk = dataset.isel(
         #   time=slice(day_chunk_start_idx,day_chunk_start_idx+ts_diff))['TMP_2maboveground']
        cat_temps = dataset[:,-1, :][day_chunk_start_idx:day_chunk_start_idx+ts_diff, :]

        # calculate stats
        tmin = np.min(cat_temps, axis=0)
        tmax = np.max(cat_temps, axis=0)
        tmean = np.mean(cat_temps, axis=0)
        lat = []
        for cat in catchments:
            lat.append(cat_lats[cat])

        pet = hargreaves(tmin, tmax, tmean, lat, ts_start)
        day_pet = np.repeat(pet[:, np.newaxis], ts_diff, axis=1)
        pet_array[:, day_chunk_start_idx:day_chunk_start_idx+ts_diff] = day_pet

        day_chunk_start_idx += 24

    pet_array = np.transpose(pet_array)  # transpose to time, cat
    dataset = np.insert(dataset, 2, pet_array, axis=1)
    return dataset

def get_lats(gdf_path: str) -> dict:

    '''
    Identify latitudes of catchments from geopackage file.

    Parameters:
        gdf_path : str
            Path to geopackage file with catchment divides layer.

    Returns:
        dict : {catchment_id: latitude}
    '''
    gdf = gpd.read_file(gdf_path, layer="divides")
    cats = gdf['divide_id']

    # convert to a geographic crs so we get actual degrees for lat/lon
    gdf_geog = gdf.to_crs(4326)
    with warnings.catch_warnings(): # it will complain about it being a
        # geographic CRS, this is to shut it up
        warnings.simplefilter("ignore")
        lats = gdf_geog.centroid.y

    cat_lat = dict(zip(cats, lats))
    return cat_lat

def multiprocess_get_lats(files: list, max_procs: int) -> dict:
    '''
    Multiprocess get latitudes from multiple geopackage files.

    Parameters:
        files : list
            List of paths to geopackage files.
        max_procs : int
            Maximum number of processes to use.

    Returns:
        dict : {catchment_id: latitude}
    '''
    lat_dicts = []
    with cf.ProcessPoolExecutor(max_workers=max_procs) as pool:
        for results in pool.map(
        get_lats,
        files
        ):
            lat_dicts.append(results)

    cat_latitudes = {}
    for jdict in lat_dicts:
        cat_latitudes.update(jdict)

    return cat_latitudes
