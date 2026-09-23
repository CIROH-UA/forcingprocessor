"""Main forcingprocessor module.

prep_ngen_data is the whole run, one named step per line. Every step is defined in steps.py, and
anything that varies by run type was bound to the configuration as a Mode during the first step.
"""

import argparse
import json
import subprocess
import time

from forcingprocessor import steps
from forcingprocessor.utils import Profiler

PROFILE_LOG = "./profile_fp.txt"


def prep_ngen_data(conf: dict) -> None:
    """
    Primary function to retrieve forcing data and convert it into files that can be ingested into
    ngen. See https://github.com/CIROH-UA/forcingprocessor/blob/main/README.md.

    Args:
        conf (dict): forcingprocessor config file
            https://github.com/CIROH-UA/forcingprocessor/blob/main/configs/conf_fp.json, read as a
            dict

    Raises:
        TypeError: Raised when plotter is not configured properly
    """
    t_start = time.perf_counter()
    profiler = Profiler(log_file=PROFILE_LOG, timings={})
    profiler.log("FORCINGPROCESSOR_START")

    cfg, layout, nwm_meta = steps.configure(conf, profiler)
    geom = steps.load_geometry(cfg, profiler)
    s3_client = steps.store_inputs(cfg, layout, geom, profiler)
    extracted = steps.extract(cfg, geom, profiler)
    extracted, nwm_meta = steps.order_forward_in_time(extracted, nwm_meta)
    written = steps.write_outputs(cfg, layout, geom, nwm_meta, extracted, profiler)
    core_runtime = time.perf_counter() - t_start

    steps.plot_outputs(cfg, layout, extracted, written)
    steps.collect_metadata(
        cfg, layout, s3_client, geom, extracted, written, core_runtime, profiler
    )
    extracted.release()  # release data to manage memory
    steps.write_tarballs(cfg, layout, geom, written, profiler)

    if cfg.ii_verbose:
        steps.print_summary(layout, profiler.timings, t_start)
    steps.archive_profile_log(cfg, layout, s3_client, profiler)


def main():
    """Read config json file and run through all forcingprocessor steps."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="infile",
        type=str,
        help="A json containing user inputs to run forcingprocessor",
    )
    args = parser.parse_args()

    if args.infile[0] == "{":
        conf = json.loads(args.infile)
    else:
        if "s3://" in args.infile:
            subprocess.run(["wget", f"{args.infile}"], check=True)
            filename = args.infile.split("/")[-1]
            with open(filename, encoding="utf-8") as f:
                conf = json.load(f)
        else:
            with open(args.infile, encoding="utf-8") as f:
                conf = json.load(f)

    prep_ngen_data(conf)


if __name__ == "__main__":
    main()
