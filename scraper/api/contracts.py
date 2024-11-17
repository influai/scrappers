from pydantic import BaseModel, Field


class ChannelList(BaseModel):
    channels: list[str] = Field(
        description="List of Telegram channels name in format: '@channel_name'."
    )
