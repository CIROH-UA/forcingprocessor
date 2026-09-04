"""Tools for plotting NGEN and NWM forcings side-by-side as gifs."""

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import imageio.v2 as imageio
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from forcingprocessor.utils import get_window, ngen_variables, nwm_variables
from forcingprocessor.weights_hf2ds import hf2ds

plt.style.use("dark_background")
mpl.use("Agg")


def plot_ngen_forcings(
    nwm_data: np.ndarray[tuple[int, int, int, int], np.dtype[np.float64]],
    ngen_data: np.ndarray[tuple[int, int, int], np.dtype[np.float64]],
    geopackage: str,
    t_ax: list[str] | pd.Series,
    catchment_ids: list[str] | np.ndarray,
    ngen_vars_plot: list[str] | None = None,
    output_dir: Path = Path("./GIFs"),
) -> None:
    """
    Generates side-by-side gif of nwm and ngen forcing data

    Args:
        nwm_data (np.ndarray) : 4d array (time x nwm_forcing_variable x west_east x south_north)
        ngen_data (np.ndarray): 3d array (time x ngen_forcing_variable x catchment)
        geopackage (str): path to geopackage containing the divides layer
        t_ax (list | pd.Series): list of datetimes for the time axis
        catchment_ids (list | np.ndarray): list of catchment ids
        ngen_vars_plot (list | None): list of ngen variables to plot
        output_dir (Path): directory to save the generated gifs. Defaults to "./GIFs".
    """

    if ngen_vars_plot is None:
        ngen_vars_plot = ngen_variables
    gdf = gpd.read_file(geopackage, layer="divides")
    gdf = gdf.set_index("divide_id")
    gdf = gdf.reindex(catchment_ids)
    jplot_vars = np.array(
        [x for x in range(len(ngen_variables)) if ngen_variables[x] in ngen_vars_plot]
    )
    cmin = 0
    cmax = 0
    for var_idx, ngen_variable in enumerate(ngen_vars_plot):
        nwm_variable = nwm_variables[jplot_vars[var_idx]]
        print(f"creating gif for variables {nwm_variable} -> {ngen_variable}")
        images = []
        for j, jtime in enumerate(t_ax):
            _, axes = plt.subplots(1, 2, figsize=(8, 8), dpi=200)
            nwm_data_jvar = nwm_data[j, var_idx, :, :]
            if j == 0:
                cmin = np.min(nwm_data_jvar)
                cmax = np.max(nwm_data_jvar)
            im = axes[0].imshow(nwm_data_jvar, vmin=cmin, vmax=cmax)
            axes[0].axis("off")
            axes[0].set_title("NWM")
            gdf[ngen_variable] = ngen_data[j, var_idx, :]
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
            plt.suptitle(f"{domain} {jtime}")
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
    forcings_nc: os.PathLike, requested_vars: list | None = None
) -> tuple[np.ndarray, list, np.ndarray]:
    """
    Converts a netcdf of ngen forcings to a 3d array (time x ngen_forcing_variable x catchment).

    Args:
        forcings_nc (os.PathLike): path to ngen forcings netcdf
        requested_vars (list | None, optional): List of variables to include. Defaults to all
            variables.

    Returns:
        tuple[np.ndarray, list, np.ndarray]: 3d array of ngen forcings, time axis, and catchment
            ids.
    """
    if requested_vars is None:
        requested_vars = ngen_variables
    with xr.open_dataset(forcings_nc) as ngen_forcings:
        ngen_data = np.zeros(
            (len(ngen_forcings.time), len(requested_vars), len(ngen_forcings.ids)),
            dtype=np.float32,
        )
        t_ax = ngen_forcings["Time"].to_numpy()[0, :]
        catchment_ids = ngen_forcings["ids"].to_numpy()
        for j, jvar in enumerate(requested_vars):
            ngen_data[:, j, :] = np.moveaxis(
                np.array(ngen_forcings[jvar]), [0, 1], [1, 0]
            )

    t_ax_dt = []
    for jt in list(t_ax):
        t_ax_dt.append(datetime.fromtimestamp(jt, tz=UTC).strftime("%Y%m%d%H%M"))

    return ngen_data, t_ax_dt, catchment_ids


def csvs_to_3darray(
    forcings_dir: os.PathLike,
    requested_vars: list | None = None,
) -> tuple[np.ndarray, pd.Series, list]:
    """
    Converts a directory of ngen forcings csvs to a 3d array (time x ngen_forcing_variable x
    catchment).

    Args:
        forcings_dir (os.PathLike): path to directory containing ngen forcings csvs
        requested_vars (list | None, optional): List of variables to include. Defaults to all
            variables.

    Returns:
        tuple[np.ndarray, pd.Series, list]: 3d array of ngen forcings, time axis, and catchment ids.
    """
    if requested_vars is None:
        requested_vars = ngen_variables
    catchment_ids = []
    i = 0
    t_ax = pd.Series()
    for _, _, files in os.walk(forcings_dir):  # type: ignore
        for j, jfile in enumerate(files):
            if jfile[-3:] == "csv":
                catchment_id = jfile.split(".")[0]
                catchment_ids.append(catchment_id)
                ngen_jdf = pd.read_csv(os.path.join(forcings_dir, jfile))
                if i == 0:
                    i += 1
                    t_ax = ngen_jdf["time"]
                    ngen_jdf = ngen_jdf.drop(columns="time")
                    shp = ngen_jdf.shape
                    ngen_data = np.zeros((len(files), shp[0], shp[1]), dtype=np.float32)
                else:
                    ngen_jdf = ngen_jdf.drop(columns="time")

                ngen_data[j, :, :] = np.array(ngen_jdf)

    ngen_vars = np.array(
        [x for x in range(len(ngen_variables)) if ngen_variables[x] in requested_vars]
    )
    ngen_data = np.moveaxis(ngen_data[:, :, ngen_vars], [0, 1, 2], [2, 0, 1])

    return ngen_data, t_ax, catchment_ids


def get_nwm_data_array(
    nwm_folder: list,
    geopackage: gpd.GeoDataFrame,
    nwm_vars: list | np.ndarray | None = None,
) -> np.ndarray:
    """
    Inputs a folder of national water model files and nwm variable names to extract.

    Outputs a windowed array of national water model data for the domain and forcing variables
    specified.

    Args:
        nwm_folder (list): list of paths to nwm forcing files
        geopackage (gpd.GeoDataFrame): geopackage containing the divides layer
        nwm_vars (list | np.ndarray | None, optional): List of nwm variables to extract. Defaults to
            all NWM variables.

    Returns:
        np.ndarray: 4d array of nwm data (time x nwm_forcing_variable x west_east x south_north)
    """
    if nwm_vars is None:
        nwm_vars = nwm_variables
    weights_json, _ = hf2ds([geopackage], nwm_folder[0], 1)
    x_min, x_max, y_min, y_max = get_window(weights_json)

    for path, _, files in os.walk(nwm_folder):  # type: ignore
        nwm_data = np.zeros(
            (len(files), len(nwm_vars), y_max - y_min + 1, x_max - x_min + 1),
            dtype=np.float32,
        )
        for k, jfile in enumerate(sorted(files)):
            jfile_path = os.path.join(path, jfile)
            ds = xr.open_dataset(jfile_path)
            nwm_var = np.zeros(
                (len(nwm_vars), y_max - y_min + 1, x_max - x_min + 1), dtype=np.float32
            )
            for j, jvar in enumerate(nwm_vars):
                nwm_var[j, :, :] = np.flip(
                    np.squeeze(
                        ds[jvar].isel(
                            x=slice(x_min, x_max + 1),
                            y=slice(3840 - y_max, 3840 - y_min + 1),
                        )
                    ),
                    0,
                )
            nwm_data[k, :, :, :] = nwm_var

    return nwm_data


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
    nwm_vars_to_plot = np.array(
        [
            nwm_variables[x]
            for x in range(len(ngen_variables))
            if ngen_variables[x] in requested_ngen_variables
        ]
    )
    nwm_data_to_plot = get_nwm_data_array(
        args.nwm_folder, args.geopackge, nwm_vars_to_plot
    )

    if args.ngen_forcings.endswith(".nc"):
        ngen_data_to_plot, t_ax_to_plot, catchment_ids_to_plot = nc_to_3darray(
            args.ngen_forcings, requested_ngen_variables
        )
    else:
        ngen_data_to_plot, t_ax_to_plot, catchment_ids_to_plot = csvs_to_3darray(
            args.ngen_forcings, requested_ngen_variables
        )

    plot_ngen_forcings(
        nwm_data_to_plot,
        ngen_data_to_plot,
        args.geopackage,
        t_ax_to_plot,
        catchment_ids_to_plot,
        requested_ngen_variables,
        args.output_dir,
    )
    print("Gifs creation complete")
