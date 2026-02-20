# Forcing Processor
Forcingprocessor converts National Water Model (NWM) forcing data into Next Generation National Water Model (NextGen) forcing data. This tool provides the forcing pre-processing for the [NextGen Research DataStream](https://github.com/CIROH-UA/ngen-datastream).

The motivation for this tool is NWM data is gridded and stored within netCDFs for each forecast hour. Ngen inputs this same forcing data, but in the format of catchment averaged data time series data.

![forcing_gif](docs/gifs/T2D_2_TMP_2maboveground_cali.gif)

## Install UV early
```
    curl -LsSf https://astral.sh/uv/install.sh | sh
```
## install requirement files

```
   uv add --dev -r requirements.txt 
```
## make directory for output data
```
    mkdir -p   data/forcing
```
## Run the forcingprocessor with UV
```
uv run python src/forcingprocessor/processor.py ./configs/conf_fp.json 
```
Prior to executing the processor, the user will need to obtain a geopackage file to define the spatial domain. The user will define the time domain by generating the forcing filenames for `processor.py` via `nwm_filenames_generator.py`, which is explained [here](#nwm_file). Note that `forcingprocessor` will calcuate weights if not found within the geopackage file.

### Channel Routing Data Extraction

This tool can also extract `q_lateral` data from the NWM's channel routing data sources. This is
helpful for experimenting with routing simulations. Note that channel routing data extraction
cannot be run at the same time as forcing data extraction at this time. This use case requires a
mapping file ending in `map.json` in the format
```
{ngen-nex-id-1: [nwm-id-1-1,...],
.
.
.
ngen-nex-id-k: [nwm-id-k-1,...]}
```
where the list of `nwm-id`s are the NHD reaches associated with that NextGen hydrofabric nexus.

## Example `conf.json`
```
{
    "forcing"  : {
        "nwm_file"     : "",
        "gpkg_file"    : ""
    },

    "storage":{
        "output_path"      : "",
        "output_file_type" : []
    },

    "run" : {
        "verbose"       : true,
        "collect_stats" : true,
        "nprocs"        : 2
    },

    "plot":{
        "nts"        : 24,
        "ngen_vars"  : [
            "DLWRF_surface",
            "APCP_surface",
            "precip_rate",
            "TMP_2maboveground"
        ]
    }
}
```

## `conf.json` Options
### 1. Forcing
| Field             | Description              | Required |
|-------------------|--------------------------|----------|
| nwm_file          | Path to a text file containing nwm file names. One filename per line. [Tool](#nwm_file) to create this file | :white_check_mark: |
| gpkg_file       | Geopackage file to define spatial domain. Use [hfsubset](https://github.com/lynker-spatial/hfsubsetCLI) to generate a geopackage with a `forcing-weights` layer. Accepts local absolute path, s3 URI or URL. Also acceptable is a weights parquet generated with [weights_hf2ds.py](https://github.com/CIROH-UA/forcingprocessor/blob/main/src/forcingprocessor/weights_hf2ds.py), though the plotting option will no longer be available. |  :white_check_mark: |
| map_file          | Path to a json containing the NWM to NGEN mapping for channel routing data extraction. Absolute path or s3 URI |  |

### 2. Storage

| Field             | Description                       | Required |
|-------------------|-----------------------------------|----------|
| storage_type      | Type of storage (local or s3 URI)     | :white_check_mark: |
| output_path       | Path to write data to. Accepts local path or s3 URI | :white_check_mark: |
| output_file_type  | List of output file types, e.g. ["tar","parquet","csv","netcdf"]  | :white_check_mark: |

### 3. Run
| Field             | Description                    | Required |
|-------------------|--------------------------------|----------|
| verbose           | Get print statements, defaults to false           |  :white_check_mark: |
| collect_stats     | Collect forcing metadata, defaults to true       |  :white_check_mark: |
| nprocs      | Number of data processing processes, defaults to 50% available cores |   |

### 4. Plot
Use this field to create a side-by-side gif of the nwm and ngen forcings
| Field             | Description                    | Required |
|-------------------|--------------------------------|----------|
| nts           | Number of timesteps to include in the gif, default is 10           |   |
| ngen_vars     | Which ngen forcings variables to create gifs of, default is all of them  |   |`
```
ngen_variables = [
    "UGRD_10maboveground",
    "VGRD_10maboveground",
    "DLWRF_surface",
    "APCP_surface",
    "precip_rate",
    "TMP_2maboveground",
    "SPFH_2maboveground",
    "PRES_surface",
    "DSWRF_surface",
]
```

## nwm_file
A text file given to forcingprocessor that contains each nwm forcing file name. These can be URLs or local paths. This file can be generated with the [nwmurl tool](https://github.com/CIROH-UA/nwmurl) and a [generator script](https://github.com/CIROH-UA/forcingprocessor/blob/main/src/forcingprocessor/nwm_filenames_generator.py) has been provided within this repo. The config argument accepts an s3 URL.
 ```
 python nwm_filenames_generator.py conf_nwm_files.json
 ```
 An example configuration file:
 ```
 {
    "forcing_type" : "operational_archive",
    "start_date"   : "202310300000",
    "end_date"     : "202310300000",
    "runinput"     : 1,
    "varinput"     : 5,
    "geoinput"     : 1,
    "meminput"     : 0,
    "urlbaseinput" : 7,
    "fcst_cycle"   : [0],
    "lead_time"    : [1]
}
 ```

## Weights
To calculate NextGen forcings, "weights" must be calculated to extract polygon averaged data from gridded data. The weights are made up of two parts, the `cell_id` and `coverage`. These are calculated via [exactextract](https://github.com/isciences/exactextract) within [weights_hf2ds.py](https://github.com/CIROH-UA/forcingprocessor/blob/main/src/forcingprocessor/weights_hf2ds.py), which is optionally called from forcingprocessor.

If a geopackage is supplied to forcingprocessor, it will be searched for the layer `forcings-weights`. If this layer is found, these weights are used during processing. If not, forcingprocessor will call [weights_hf2ds.py](https://github.com/CIROH-UA/forcingprocessor/blob/main/src/forcingprocessor/weights_hf2ds.py) to calculate the weights (cell_id and coverage) for every divide-id in the geopackage. This can take time, so forcingprocessor will write a parquet of weights out in the metadata, that can be reused in future forcingprocessor executions.

Example of direct call
```
python3 forcingprocessor/src/forcingprocessor/weights_hf2ds.py \
--outname ./weights.parquet \
--input_file ./nextgen_VPU_03W.gpkg
```