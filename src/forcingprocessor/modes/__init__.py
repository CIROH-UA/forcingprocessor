"""The three things forcingprocessor produces, and the one place the choice between them is made.

Each mode module supplies one implementation of every field on Mode, which is defined alongside the
other run records in records.py.
"""

import re

from forcingprocessor.records import Mode, NWMFileMetadata

__all__ = ["Mode", "parse_cycle_and_lead", "select_mode"]


def parse_cycle_and_lead(files: list[str], pattern: str) -> NWMFileMetadata:
    """Extract forecast cycle and lead times from the first and last file names.

    Args:
        files (list[str]): The NWM files this run reads.
        pattern (str): Regex matching this run type's file naming convention.

    Returns:
        NWMFileMetadata: Information about the NWM data sourced from the URL.
    """
    meta = NWMFileMetadata()
    match = re.search(pattern, files[0])
    if match:
        meta.urlbase = match.group(2)
        meta.fcst_cycle = match.group(3) + match.group(4)
        meta.lead_start = match.group(5) + match.group(6)
    else:
        print(
            "Could not extract forecast cycle and lead start from the first NWM forcing file: "
            + f"{files[0]}"
        )

    match = re.search(pattern, files[-1])
    if match:
        meta.lead_end = match.group(5) + match.group(6)
    else:
        print(f"Could not extract lead end from the last NWM forcing file: {files[-1]}")

    return meta


def select_mode(
    map_file: str | None = None, restart_map_file: str | None = None
) -> Mode:
    """Resolve the run type from the config, once.

    Args:
        map_file (str | None): NWM to NGEN json map, present for channel routing runs.
        restart_map_file (str | None): NGEN to NWM json map, present for t-route restart runs.

    Returns:
        Mode: The bound implementation of every step that varies by run type.
    """
    # Imported here so the mode modules are free to import from this one.
    from forcingprocessor.modes import (  # pylint: disable=import-outside-toplevel
        channel_routing,
        forcings,
        restarts,
    )

    if map_file:  # NWM to NGEN channel routing processing requires json map
        return channel_routing.MODE
    if restart_map_file:
        return restarts.MODE
    return forcings.MODE
