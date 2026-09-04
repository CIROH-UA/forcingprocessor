"""Reads a json configuration file and runs nwmurl to generate NWM forcing file names."""

import argparse
import json
import os

import nwmurl


def generate_nwmfiles(conf: dict) -> None:
    """Run nwmurl based on the configuration file.

    Args:
        conf (dict): Contents of a json nwmurl configuration file.
    """
    forcing_type = conf.get("forcing_type", None)

    if forcing_type == "operational_archive":
        start_date = conf.get("start_date", None)
        end_date = conf.get("end_date", None)
        fcst_cycle = conf.get("fcst_cycle", None)
        lead_time = conf.get("lead_time", None)
        varinput = conf.get("varinput", None)
        geoinput = conf.get("geoinput", None)
        runinput = conf.get("runinput", None)
        urlbaseinput = conf.get("urlbaseinput", None)
        meminput = conf.get("meminput", None)
        write_to_file = conf.get("write_to_file", True)
        nwmurl.generate_urls_operational(
            start_date,
            end_date,
            fcst_cycle,
            lead_time,
            varinput,
            geoinput,
            runinput,
            urlbaseinput,
            meminput,
            write_to_file,
        )
    elif forcing_type == "retrospective":
        start_date = conf.get("start_date", None)
        end_date = conf.get("end_date", None)
        urlbaseinput = conf.get("urlbaseinput", None)
        selected_object_type = conf.get("selected_object_type", None)
        selected_var_types = conf.get("selected_var_types", None)
        write_to_file = conf.get("write_to_file", True)
        nwmurl.generate_urls_retro(
            start_date,
            end_date,
            urlbaseinput,
            selected_object_type,
            selected_var_types,
            write_to_file,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="infile", type=str, help="A json containing user inputs to run nwmurl"
    )
    args = parser.parse_args()
    if "s3" in args.infile:
        os.system(f"wget {args.infile}")
        filename = args.infile.split("/")[-1]
        with open(filename, encoding="utf-8") as f:
            config = json.load(f)
    else:
        with open(args.infile, encoding="utf-8") as f:
            config = json.load(f)
    generate_nwmfiles(config)
