"""forcingprocessor: NWM gridded output into ngen-ready catchment files.

The package is layered, outermost first:

    processor            the workflow, one named step per line
    steps                what each step is for
    config metadata
    writers weights_hf2ds
    averaging weights    the science kernels, and the contributor extension points
    modes                the three run types, bound once by read_config
    records utils        shared data shapes and plumbing

Names exported here are the supported surface. To contribute an alternative averaging or weight
generation scheme, start at averaging.py and weights.py.
"""

from importlib import import_module

from ._version import __version__ as __version__

_EXPORTS = {
    # Workflow
    "prep_ngen_data": "forcingprocessor.processor",
    "main": "forcingprocessor.processor",
    # Configuration
    "read_config": "forcingprocessor.config",
    # Kernels: the contributor extension points
    "WindowedWeights": "forcingprocessor.averaging",
    "average_to_catchments": "forcingprocessor.averaging",
    "prepare_weights": "forcingprocessor.averaging",
    "Window": "forcingprocessor.weights",
    "calc_weights_from_gdf": "forcingprocessor.weights",
    "grid_window": "forcingprocessor.weights",
    "normalize_weight_table": "forcingprocessor.weights",
    # Weight table loading
    "hf2ds": "forcingprocessor.weights_hf2ds",
    "multiprocess_hf2ds": "forcingprocessor.weights_hf2ds",
    # Run types
    "select_mode": "forcingprocessor.modes",
    # Records passed between steps
    "Extracted": "forcingprocessor.records",
    "Geometry": "forcingprocessor.records",
    "Mode": "forcingprocessor.records",
    "NWMFileMetadata": "forcingprocessor.records",
    "OutputLayout": "forcingprocessor.records",
    "RunConfig": "forcingprocessor.records",
    "WriteResult": "forcingprocessor.records",
    # Shared vocabulary
    "ngen_variables": "forcingprocessor.utils",
    "normalize_vpu_id": "forcingprocessor.utils",
    "nwm_variables": "forcingprocessor.utils",
    "vpus": "forcingprocessor.utils",
}

__all__ = ["__version__", *sorted(_EXPORTS)]


def __getattr__(name):
    """Resolve exports on first use so importing the package stays cheap."""
    if name in _EXPORTS:
        return getattr(import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
