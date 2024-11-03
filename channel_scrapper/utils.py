import logging
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import yaml
from telethon.errors import RpcCallFailError

# Date format used in the configuration file
DATE_FORMAT = "%d-%m-%Y"


async def safe_api_request(coroutine, comment):
    """
    Safely executes an asynchronous API request, handling exceptions.

    Args:
        coroutine (Awaitable): The asynchronous request to execute.
        comment (str): Description of the action for logging purposes.

    Returns:
        The result of the coroutine if successful, otherwise None.
    """
    try:
        return await coroutine
    except RpcCallFailError as e:
        logging.error(f"[!] Telegram API error, {comment}: {str(e)}")
    except Exception as e:
        logging.error(f"[!] General error, {comment}: {str(e)}")
    return None


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


def format_channel_url(tg_name) -> None | str:
    """Convert @channel_name format to https://t.me/channel_name URL."""
    if pd.isna(tg_name) or not tg_name:
        return None
    if tg_name.startswith("@"):
        return f"https://t.me/{tg_name[1:]}"
    elif tg_name.startswith("https://t.me/"):
        return tg_name
    return None
