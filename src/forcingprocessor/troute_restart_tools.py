# TODO: define the functions

def make_troute_restart():
    # TODO: define the function
    """
    What the function will do:
    1. Define a depth solving function that takes streamflow, velocity, top width, bottom width, and
    channel slope as parameters
    2. Read an NextGen catchment to NHD reach mapping, a "crosswalk" netcdf file that has all the
    NextGen catchments in the order that the restart file will have them in, the NWM analysis
    assimilation netcdf, and the NWM routelink file (contains channel geometries)
    3. Preprocess those files
    4. Take averages of all the depth function parameters. One NextGen catchment can be associated
    with multiple NHD reaches, so we average the values for all the associated NHD reaches to get
    the value for the NextGen catchment to be passed into the depth function
    5. Solve for depths
    6. Create xarray dataset
    """
    pass