from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# Date format used in the configuration file
DATE_FORMAT = "%d-%m-%Y"


def load_configs(config_path: Path) -> tuple[datetime, datetime]:
    """
    Loads configurations from a YAML file.

    Args:
        config_path (Path): Path to the YAML configuration file.

    Returns:
        tuple: from_date, to_date loaded from the config file.
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Convert dates from string to datetime with UTC timezone
    from_date = datetime.strptime(config["parsing"]["from_date"], DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )
    to_date = datetime.strptime(config["parsing"]["to_date"], DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )

    return from_date, to_date


def format_channel_name(tg_name) -> None | str:
    """Check and convert @channel_name format to just channel_name."""
    if pd.isna(tg_name) or not tg_name:
        return None
    if tg_name.startswith("@"):
        return tg_name[1:]
    return None