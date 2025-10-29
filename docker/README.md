# Docker README

## Overview

Forcingprocessor converts National Water Model (NWM) forcing data into NextGen-compatible formats using a two-stage Docker build.

**Images:**
- `awiciroh/forcingprocessor-deps` - Dependencies and compiled libraries
- `awiciroh/forcingprocessor` - Application code

**Architectures:**
- x86_64: `latest-x86`
- ARM64: `latest-arm64`

## Quick Start
```bash
# Pull image
docker pull awiciroh/forcingprocessor:latest-x86

# Run processor
docker run -v $(pwd)/configs:/config \
           -v $(pwd)/output:/output \
           awiciroh/forcingprocessor:latest-x86 \
           python /forcingprocessor/src/forcingprocessor/processor.py /config/conf_fp.json
```

## Build from Source
```bash
# x86_64
TAG=latest-x86 docker compose -f docker/docker-compose.yml build

# ARM64
ARCH=aarch64 TAG=latest-arm64 docker compose -f docker/docker-compose.yml build forcingprocessor-deps
TAG=latest-arm64 docker compose -f docker/docker-compose.yml build forcingprocessor
```

## Images

### forcingprocessor-deps
- **Base:** amazonlinux:2023
- **x86_64:** Python 3.9, pip, git, pigz, tar
- **ARM64:** Additional compiled libraries (PROJ 9.3.1, GDAL 3.8.3, HDF5 1.14.3, GEOS 3.12.1, exactextract)
- **Tag variable:** `TAG` (defaults to `latest`)

### forcingprocessor
- **Base:** awiciroh/forcingprocessor-deps:${TAG_NAME}
- Installs forcingprocessor Python package from `/forcingprocessor`
- Runs as non-root user (`myuser:mygroup`)
- **Build arg:** `TAG_NAME` (defaults to `latest`)

## Configuration

See [conf_fp.json](../configs/conf_fp.json) for configuration options:
- **forcing:** Input NWM files and geopackage
- **storage:** Output path and file types (csv, parquet, netcdf, tar)
- **run:** Verbosity, stats collection, number of processes

## Common Commands
```bash
# With AWS credentials
docker run -v ~/.aws:/root/.aws:ro \
           -v $(pwd)/configs:/config \
           -v $(pwd)/output:/output \
           awiciroh/forcingprocessor:latest-x86 \
           python /forcingprocessor/src/forcingprocessor/processor.py /config/conf_fp.json

# With environment variables
docker run -e AWS_ACCESS_KEY_ID \
           -e AWS_SECRET_ACCESS_KEY \
           -e AWS_DEFAULT_REGION=us-east-1 \
           -v $(pwd)/configs:/config \
           awiciroh/forcingprocessor:latest-x86 \
           python /forcingprocessor/src/forcingprocessor/processor.py /config/conf_fp.json

# Generate NWM file list
docker run -v $(pwd)/configs:/config \
           awiciroh/forcingprocessor:latest-x86 \
           python /forcingprocessor/src/forcingprocessor/nwm_filenames_generator.py \
           /config/conf_nwmurl_operational.json

# Interactive shell
docker run -it awiciroh/forcingprocessor:latest-x86 bash
```

## Resources

- **Docker Hub:** https://hub.docker.com/u/awiciroh
- **GitHub:** https://github.com/CIROH-UA/forcingprocessor
