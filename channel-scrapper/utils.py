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


def load_configs(config_path: Path) -> tuple[dict, str, datetime, datetime]:
    """
    Loads configurations from a YAML file.

    Args:
        config_path (Path): Path to the YAML configuration file.

    Returns:
        tuple: tg_client, channel_url, from_date, to_date loaded from the config file.
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    tg_client = config["tg_client"]
    channel_url = config["parsing"]["channel_url"]

    # Convert dates from string to datetime with UTC timezone
    from_date = datetime.strptime(config["parsing"]["from_date"], DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )
    to_date = datetime.strptime(config["parsing"]["to_date"], DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )

    logging.info(f"Configs successfully loaded, parsing channel: {channel_url}")

    return tg_client, channel_url, from_date, to_date


def create_or_load_csv(file_name: Path) -> pd.DataFrame:
    """
    Creates or loads a CSV file to store runs information.

    Args:
        file_name (Path): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing runs information.
    """
    if file_name.exists():
        df = pd.read_csv(file_name)
        logging.info(f"Loaded existing CSV file with runs info: {file_name}")
    else:
        # Define columns for new CSV
        df = pd.DataFrame(
            columns=[
                "channel_url",
                "from_date",
                "to_date",
                "posts_scrapped",
                "launch_time",
                "exec_time",
            ]
        )
        df.to_csv(file_name, index=False)
        logging.info(f"Created new CSV file with runs info: {file_name}")

    return df


def last_old_dates(
    runs_info: pd.DataFrame, channel_url: str
) -> tuple[None, None] | tuple[datetime, datetime]:
    """
    Finds the latest 'to_date' and oldest 'from_date' for a specific channel URL.

    Args:
        runs_info (pd.DataFrame): DataFrame containing historical runs information.
        channel_url (str): The specific Telegram channel URL.

    Returns:
        tuple: (latest_to_date, oldest_from_date) or (None, None) if no data is found.
    """
    # Filter data for the specific channel URL
    channel_data = runs_info[runs_info["channel_url"] == channel_url]

    if channel_data.empty:
        return None, None

    # Convert date columns to datetime with UTC timezone
    channel_data["from_date"] = pd.to_datetime(channel_data["from_date"]).dt.tz_convert(
        timezone.utc
    )
    channel_data["to_date"] = pd.to_datetime(channel_data["to_date"]).dt.tz_convert(
        timezone.utc
    )

    oldest_from_date = channel_data["from_date"].min()
    latest_to_date = channel_data["to_date"].max()

    return latest_to_date, oldest_from_date


def save_run(
    runs_info: pd.DataFrame,
    channel_url: str,
    from_date: datetime,
    to_date: datetime,
    posts_scrapped: int,
    launch_time: str,
    exec_time: float,
) -> pd.DataFrame:
    """
    Saves run information (metadata about the scraping run) to the DataFrame.

    Args:
        runs_info (pd.DataFrame): DataFrame containing previous runs.
        channel_url (str): The Telegram channel URL.
        from_date (datetime): The start date for the scraping.
        to_date (datetime): The end date for the scraping.
        posts_scrapped (int): Number of posts scraped during the run.
        launch_time (str): Time the scraping run was launched.
        exec_time (float): Total execution time of the scraping run.

    Returns:
        pd.DataFrame: Updated DataFrame with the new run information.
    """
    # Create a dictionary with run data
    run_data = {
        "channel_url": channel_url,
        "from_date": from_date,
        "to_date": to_date,
        "posts_scrapped": posts_scrapped,
        "launch_time": launch_time,
        "exec_time": exec_time,
    }

    # Convert the dictionary to a DataFrame
    new_run_df = pd.DataFrame([run_data])

    # Append new run data to existing DataFrame
    if runs_info.empty:
        runs_info = new_run_df
    else:
        runs_info = pd.concat([runs_info, new_run_df], ignore_index=True)

    logging.info("Run information saved successfully.")

    return runs_info
