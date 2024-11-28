import logging
import time

from telethon.errors import FloodWaitError
from telethon.functions import channels


class FloodCaretaker:
    """
    This class introduced as an attempt to eliminate FloodWaitError on the ResolveUsername method,
    which is called in ChannelScraper inside get_input_entity in the get_peer method.
    The idea is as follows: we introduce a delay between any calls to get_input_entity.
    If a FloodWaitError still occurs, we remember the time of the error and do not call the get_input_entity method.

    This way, scraper will still be able to perform other tasks, even during a ban,
    provided that the corresponding Peer for the channel is available in the database
    (then there is no need to call get_input_entity)
    """

    def __init__(self, get_inp_ent_delay) -> None:
        self.get_inp_ent_delay = get_inp_ent_delay  # delay between get_input_entity calls, in seconds
        self.last_get_inp_ent_call = None  # last time when get_input_entity called, in seconds
        self.fwe_delay = None  # delay on the ability to call get_input_entity was imposed by FloodWaitError, in seconds
        self.last_fwe = None  # last time when FloodWaitError occurred, in seconds

    def check(self) -> None:
        """
        Validates whether it is safe to call get_input_entity.
        """
        # Handle FloodWaitError wait state
        if self.fwe_delay is not None:
            time_since_last_fwe = time.time() - self.last_fwe
            remaining_wait_fwe = self.fwe_delay - time_since_last_fwe

            if time_since_last_fwe < self.fwe_delay:
                logging.error(
                    f"FloodWaitError active. Wait for {remaining_wait_fwe:.2f} seconds before retrying."
                )
                raise FloodWaitError(
                    request=channels.GetFullChannelRequest,
                    capture=int(remaining_wait_fwe),
                )

        # Enforce delay between successive get_input_entity calls
        if self.last_get_inp_ent_call is not None:
            time_since_last_call = time.time() - self.last_get_inp_ent_call
            remaining_wait_call = self.get_inp_ent_delay - time_since_last_call

            if time_since_last_call < self.get_inp_ent_delay:
                logging.info(
                    f"Waiting {remaining_wait_call:.2f} seconds before calling ResolveUsername..."
                )
                time.sleep(remaining_wait_call)

        # Update the timestamp for the latest call
        self.last_get_inp_ent_call = time.time()

    def add_fwe(self, fwe_delay: float) -> None:
        """
        Records a FloodWaitError event.
        """
        self.fwe_delay = fwe_delay
        self.last_fwe = time.time()
