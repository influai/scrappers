# The goal of this function is to get the StringSession object for the TG client for scraping
# This is needed in order to not pass the authentification every time, when starting the scraper
# When the StringSession is printed in stdout, just copy it to the docker-compose in 'session' field
# Ensure that the creds are similar to which are setted in the docker-compose
# https://docs.telethon.dev/en/stable/concepts/sessions.html

from telethon.sessions import StringSession
from telethon.sync import TelegramClient

creds = {
    "TG_API_ID": 123456,  # obtain this from my.telegram.org
    "TG_API_HASH": "qwer1234",  # obtain this from my.telegram.org
    "TG_DEVICE_MODEL": "X540UAR",  # can left such value
    "TG_SYSTEM_VERSION": "Linux ubuntu GNOME Wayland glibc 2.39",  # can left such value
    "TG_APP_VERSION": "5.6.3 Snap",  # can left such value
}

with TelegramClient(
    StringSession(),
    api_id=int(creds["TG_API_ID"]),
    api_hash=creds["TG_API_HASH"],
    device_model=creds["TG_DEVICE_MODEL"],
    system_version=creds["TG_SYSTEM_VERSION"],
    app_version=creds["TG_SYSTEM_VERSION"],
) as client:
    print(client.session.save())
