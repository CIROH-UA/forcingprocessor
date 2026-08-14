import argparse
import os
from pathlib import Path
from datetime import datetime

import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
import imageio.v2 as imageio
import numpy as np
import pandas as pd
import geopandas as gpd

from forcingprocessor.weights_hf2ds import hf2ds
from forcingprocessor.utils import get_window, nwm_variables, ngen_variables

plt.style.use("dark_background")
mpl.use("Agg")


def plot_ngen_forcings(
    nwm_data_arg: np.ndarray,
    ngen_data_arg: np.ndarray,
    geopackage: str,
    t_ax_arg: list | pd.Series,
    catchment_ids_arg: list | np.ndarray,
    ngen_vars_plot: list | None,
    output_dir: Path = Path("./GIFs"),
) -> None:
    """
    Generates side-by-side gif of nwm and ngen forcing data

    Args:
        nwm_data_arg (np.ndarray) : 4d array (time x nwm_forcing_variable x west_east x south_north)
        ngen_data_arg (np.ndarray) : 3d array (time x ngen_forcing_variable x catchment)
        geopackage (str) : path to geopackage from which the data was derived
        t_ax_arg (list | pd.Series) : list of datetimes for the time axis
        catchment_ids_arg (list | np.ndarray) : list of catchment ids
        ngen_vars_plot (list | None) : list of ngen variables to plot
        output_dir (Path) : path to output directory for gifs
    """

    if not ngen_vars_plot:
        ngen_vars_plot = ngen_variables
    gdf = gpd.read_file(geopackage, layer="divides")
    gdf = gdf.set_index("divide_id")
    gdf = gdf.reindex(catchment_ids_arg)
    jplot_vars = np.array(
        [x for x in range(len(ngen_variables)) if ngen_variables[x] in ngen_vars_plot]
    )
    for var_idx, ngen_variable in enumerate(ngen_vars_plot):
        nwm_variable = nwm_variables[jplot_vars[var_idx]]
        print(f"creating gif for variables {nwm_variable} -> {ngen_variable}")
        images = []
        cmin = 0
        cmax = 0
        for j, jtime in enumerate(t_ax_arg):
            _, axes = plt.subplots(1, 2, figsize=(8, 8), dpi=200)
            nwm_data_jvar = nwm_data_arg[j, var_idx, :, :]
            if j == 0:
                cmin = np.min(nwm_data_jvar)
                cmax = np.max(nwm_data_jvar)
            im = axes[0].imshow(nwm_data_jvar, vmin=cmin, vmax=cmax)
            axes[0].axis("off")
            axes[0].set_title("NWM")
            gdf[ngen_variable] = ngen_data_arg[j, var_idx, :]
            gdf.plot(column=ngen_variable, ax=axes[1], vmin=cmin, vmax=cmax)
            axes[1].set_title("NGEN")
            axes[1].axis("off")
            fig_name = f"{jtime}.png"
            plt.colorbar(
                im,
                ax=axes,
                orientation="horizontal",
                fraction=0.1,
                label=f"{nwm_variable} -> {ngen_variable}",
            )

            domain = os.path.basename(geopackage).split(".")[0]
            plt.suptitle(f"{domain} {t_ax_arg[j]}")
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            plt.savefig(os.path.join(output_dir, fig_name))
            plt.close()
            jpng = os.path.join(output_dir, fig_name)
            images.append(imageio.imread(jpng))
            os.remove(jpng)
        imageio.mimsave(
            os.path.join(output_dir, f"{nwm_variable}_2_{ngen_variable}.gif"),
            images,
            loop=0,
            fps=2,
        )


def nc_to_3darray(
    forcings_nc: os.PathLike, requested_vars: list | None
) -> tuple[np.ndarray, list, np.ndarray]:
    """
    Convert netCDF of ngen forcings to 3 arrays

    Args:
        forcings_nc (os.PathLike): path to ngen forcings netcdf
        requested_vars (list | None): list of variables to request

    Returns:
        tuple[np.ndarray, list, np.ndarray]: array of requested variables, list of times, array of
            catchment ids
    """
    if not requested_vars:
        requested_vars = ngen_variables
    with xr.open_dataset(forcings_nc) as ngen_forcings:
        ngen_req_vars = np.zeros(
            (len(ngen_forcings.time), len(requested_vars), len(ngen_forcings.ids)),
            dtype=np.float32,
        )
        times = ngen_forcings["Time"].to_numpy()[0, :]
        catchment_ids_array = ngen_forcings["ids"].to_numpy()
        for j, jvar in enumerate(requested_vars):
            ngen_req_vars[:, j, :] = np.moveaxis(
                np.array(ngen_forcings[jvar]), [0, 1], [1, 0]
            )

    t_ax_dt = []
    for jt in list(times):
        t_ax_dt.append(datetime.fromtimestamp(jt).strftime("%Y%m%d%H%M"))

    return ngen_req_vars, t_ax_dt, catchment_ids_array


def csvs_to_3darray(
    forcings_dir: os.PathLike, requested_vars: list | None
) -> tuple[np.ndarray, pd.Series, list]:
    """
    Convert ngen forcings csvs to 3 arrays

    Args:
        forcings_dir (os.PathLike): directory containing ngen forcings csvs
        requested_vars (list | None): list of variables to request

    Returns:
        tuple[np.ndarray, pd.Series, list]: array of requested variables, series of times, list of catchment ids
    """
    if not requested_vars:
        requested_vars = ngen_variables

    catchment_ids_list = []
    i = 0
    times = pd.Series()
    for _, _, files in os.walk(forcings_dir):
        for j, jfile in enumerate(files):
            if jfile[-3:] == "csv":
                catchment_id = jfile.split(".")[0]
                catchment_ids_list.append(catchment_id)
                ngen_jdf = pd.read_csv(os.path.join(forcings_dir, jfile))
                if i == 0:
                    i += 1
                    times = ngen_jdf["time"]
                    ngen_jdf = ngen_jdf.drop(columns="time")
                    shp = ngen_jdf.shape
                    ngen_data_array = np.zeros((len(files), shp[0], shp[1]), dtype=np.float32)
                else:
                    ngen_jdf = ngen_jdf.drop(columns="time")

            ngen_data_array[j, :, :] = np.array(ngen_jdf)

    ngen_vars = np.array(
        [x for x in range(len(ngen_variables)) if ngen_variables[x] in requested_vars]
    )
    ngen_data_array = np.moveaxis(ngen_data_array[:, :, ngen_vars], [0, 1, 2], [2, 0, 1])

    return ngen_data_array, times, catchment_ids_list


def get_nwm_data_array(
    nwm_folder: str,
    geopackage: gpd.GeoDataFrame,
    nwm_vars_arg: np.ndarray | list | None
) -> np.ndarray:
    """
    Inputs a folder of national water model files and nwm variable names to extract.

    Outputs a windowed array of national water model data for the domain and forcing variables
    specified.

    Args:
        nwm_folder (str): path to folder containing nwm netcdf files
        geopackage (gpd.GeoDataFrame): geopackage from which data was derived
        nwm_vars_arg (np.ndarray | list | None): list of nwm variables to extract.
    nwm_data  : 4d array (time x nwm_forcing_variable x west_east x south_north)
    """
    if nwm_vars_arg is None:
        nwm_vars_arg = nwm_variables
    weights_json, _ = hf2ds([geopackage], nwm_folder[0], 1)
    x_min, x_max, y_min, y_max = get_window(weights_json)

    for path, _, files in os.walk(nwm_folder):
        nwm_data_array = np.zeros(
            (len(files), len(nwm_vars_arg), y_max - y_min + 1, x_max - x_min + 1),
            dtype=np.float32,
        )
        for k, jfile in enumerate(sorted(files)):
            jfile_path = os.path.join(path, jfile)
            ds = xr.open_dataset(jfile_path)
            nwm_var = np.zeros(
                (len(nwm_vars_arg), y_max - y_min + 1, x_max - x_min + 1), dtype=np.float32
            )
            for j, jvar in enumerate(nwm_vars_arg):
                nwm_var[j, :, :] = np.flip(
                    np.squeeze(
                        ds[jvar].isel(
                            x=slice(x_min, x_max + 1),
                            y=slice(3840 - y_max, 3840 - y_min + 1),
                        )
                    ),
                    0,
                )
            nwm_data_array[k, :, :, :] = nwm_var

    return nwm_data_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ngen_forcings",
        help="Path to a folder containing ngen catchment forcings csvs or path to netcdf",
        default="",
    )
    parser.add_argument(
        "--nwm_folder",
        help="Path to a folder containing nwm CONUS forcings",
        default="",
    )
    parser.add_argument(
        "--geopackage",
        help="Path to a geopackage from which the weights were created",
        default="",
    )
    parser.add_argument(
        "--ngen_variables",
        help="Space separated list of ngen variables to gif",
        default=ngen_variables,
    )
    parser.add_argument("--output_dir", help="Path to write gifs to", default="./GIFs")
    args = parser.parse_args()

    requested_ngen_variables = args.ngen_variables.split(", ")
    nwm_vars = np.array(
        [
            nwm_variables[x]
            for x in range(len(ngen_variables))
            if ngen_variables[x] in requested_ngen_variables
        ]
    )
    nwm_data = get_nwm_data_array(args.nwm_folder, args.geopackge, nwm_vars)

    if args.ngen_forcings.endswith(".nc"):
        ngen_data, t_ax, catchment_ids = nc_to_3darray(
            args.ngen_forcings, requested_ngen_variables
        )
    else:
        ngen_data, t_ax, catchment_ids = csvs_to_3darray(
            args.ngen_forcings, requested_ngen_variables
        )

    plot_ngen_forcings(
        nwm_data,
        ngen_data,
        args.geopackage,
        t_ax,
        catchment_ids,
        requested_ngen_variables,
        args.output_dir,
    )
    print("Gifs creation complete")
