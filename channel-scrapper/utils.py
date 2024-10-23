import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


DATE_FORMAT = "%d-%m-%Y"


def load_configs(config_path: Path) -> tuple[dict, str, datetime, datetime]:
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    tg_client = config["tg_client"]
    channel_url = config["parsing"]["channel_url"]

    from_date = datetime.strptime(config["parsing"]["from_date"], DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )
    to_date = datetime.strptime(config["parsing"]["to_date"], DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )

    logging.info(f"configs successfully loaded, parsing channel: {channel_url}")

    return tg_client, channel_url, from_date, to_date


def create_or_load_csv(file_name: Path) -> pd.DataFrame:
    '''create or load csv with runs info'''
    if file_name.exists():
        df = pd.read_csv(file_name)
        logging.info(f"loaded existing csv file with runs info: {file_name}")
    else:
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
        logging.info(f"created new csv file with runs info: {file_name}")
    return df


def last_old_dates(
    runs_info: pd.DataFrame, channel_url: str
) -> tuple[None, None] | tuple[datetime, datetime]:
    '''find the lastest "to_date" and oldest "from_date" for the specific "channel_url"'''
    channel_data = runs_info[runs_info["channel_url"] == channel_url]

    if channel_data.empty:
        return None, None
    
    channel_data.loc[:, 'from_date'] = pd.to_datetime(channel_data['from_date']).dt.tz_convert(timezone.utc)
    channel_data.loc[:, 'to_date'] = pd.to_datetime(channel_data['to_date']).dt.tz_convert(timezone.utc)

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
    run_data = {
        "channel_url": channel_url,
        "from_date": from_date,
        "to_date": to_date,
        "posts_scrapped": posts_scrapped,
        "launch_time": launch_time,
        "exec_time": exec_time,
    }

    new_run_df = pd.DataFrame([run_data])

    if runs_info.empty:
        runs_info = new_run_df
    else:
        runs_info = pd.concat([runs_info, new_run_df], ignore_index=True)

    logging.info("run information saved successfully")
    return runs_info
