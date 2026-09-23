"""Catchment averaging kernel: gridded NWM values into one value per catchment.

    (values, weights) -> (nvar, ncatchment)

``values`` is a float array of shape (nvar, window.ny, window.nx), already sliced to the window and
oriented north up. ``weights`` is a WindowedWeights, a weight table whose cell ids have been
resolved to window relative indices by prepare_weights.

This module is the extension point for an alternative averaging scheme. It is pure: no run
configuration, no filesystem, no process pool, so a contributed scheme can be benchmarked against a
fixed weight table and grid. The weight table it consumes is produced by weights.py.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from forcingprocessor.weights import CONUS_NX, CONUS_NY, Window


@dataclass(frozen=True)
class WindowedWeights:
    """A weight table with grid cells resolved to window relative flat indices.

    Resolving happens once per run rather than once per file, so the index arithmetic stays out of
    the per file loop. The window and grid shape are carried along so a scheme that needs grid
    coordinates, such as bilinear interpolation, can reach them.
    """

    catchment_ids: list
    cell_index: list[np.ndarray]
    coverage: list[np.ndarray]
    coverage_sum: np.ndarray
    window: Window
    grid_shape: tuple[int, int]

    def __len__(self) -> int:
        return len(self.catchment_ids)


def prepare_weights(
    weights_df: pd.DataFrame,
    window: Window,
    grid_nx: int = CONUS_NX,
    grid_ny: int = CONUS_NY,
) -> WindowedWeights:
    """Resolve a weight table's full grid cell ids into indices into the flattened window.

    Args:
        weights_df (pd.DataFrame): Weight table indexed by catchment id with cell_id and coverage
            columns.
        window (Window): The grid subset the table falls inside.
        grid_nx (int): Width of the full NWM grid. Defaults to the CONUS grid.
        grid_ny (int): Height of the full NWM grid. Defaults to the CONUS grid.

    Returns:
        WindowedWeights: The table with cell ids resolved, ready for average_to_catchments.
    """
    cell_index = []
    coverage = []
    coverage_sum = np.zeros(len(weights_df), dtype=np.float64)

    for j, row in enumerate(weights_df.itertuples()):
        cells = np.asarray(row.cell_id)
        ix, iy = np.unravel_index(  # pylint: disable=unbalanced-tuple-unpacking
            cells,
            (grid_nx, grid_ny),  # type: ignore
            order="F",
        )
        cell_index.append(
            np.ravel_multi_index(
                (ix - window.x_min, iy - window.y_min),
                (window.nx, window.ny),  # type: ignore
                order="F",
            )
        )
        cov = np.asarray(row.coverage, dtype=np.float64)
        coverage.append(cov)
        coverage_sum[j] = np.sum(cov)

    return WindowedWeights(
        catchment_ids=list(weights_df.index),
        cell_index=cell_index,
        coverage=coverage,
        coverage_sum=coverage_sum,
        window=window,
        grid_shape=(grid_nx, grid_ny),
    )


def average_to_catchments(values: np.ndarray, weights: WindowedWeights) -> np.ndarray:
    """Coverage weighted mean of the grid cells under each catchment.

    Args:
        values (np.ndarray): Gridded NWM values of shape (nvar, window.ny, window.nx), window
            relative and oriented north up.
        weights (WindowedWeights): Prepared weight table from prepare_weights.

    Returns:
        np.ndarray: Array of shape (nvar, ncatchment), catchments in weights.catchment_ids order.
    """
    nvar = values.shape[0]
    flat = values.reshape(nvar, -1)
    out = np.zeros((nvar, len(weights)), dtype=np.float64)

    for j in range(len(weights)):
        # np.take rather than flat[:, idx]: it gathers C ordered, which fixes the order the sum
        # below accumulates in and keeps results reproducible.
        cells = np.take(flat, weights.cell_index[j], axis=1)
        out[:, j] = (
            np.sum(weights.coverage[j] * cells, axis=1) / weights.coverage_sum[j]
        )

    return out
