import json
import argparse
import subprocess

import nwmurl


def generate_nwmfiles(conf_arg: dict):
    """Read NWMURL configuration file and generate NWM file URLs.

    Args:
        conf_arg (dict): NWMURL configuration file
    """
    forcing_type = conf_arg.get("forcing_type", None)

    if forcing_type == "operational_archive":
        start_date = conf_arg.get("start_date", None)
        end_date = conf_arg.get("end_date", None)
        fcst_cycle = conf_arg.get("fcst_cycle", None)
        lead_time = conf_arg.get("lead_time", None)
        varinput = conf_arg.get("varinput", None)
        geoinput = conf_arg.get("geoinput", None)
        runinput = conf_arg.get("runinput", None)
        urlbaseinput = conf_arg.get("urlbaseinput", None)
        meminput = conf_arg.get("meminput", None)
        write_to_file = conf_arg.get("write_to_file", True)
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
        start_date = conf_arg.get("start_date", None)
        end_date = conf_arg.get("end_date", None)
        urlbaseinput = conf_arg.get("urlbaseinput", None)
        selected_object_type = conf_arg.get("selected_object_type", None)
        selected_var_types = conf_arg.get("selected_var_types", None)
        write_to_file = conf_arg.get("write_to_file", True)
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
        subprocess.run(["wget", args.infile], check=True)
        filename = args.infile.split("/")[-1]
        with open(filename, "r", encoding="utf-8") as f:
            conf = json.load(f)
    else:
        with open(args.infile, "r", encoding="utf-8") as f:
            conf = json.load(f)
    generate_nwmfiles(conf)
