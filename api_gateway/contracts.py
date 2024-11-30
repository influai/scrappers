from pydantic import BaseModel, Field


class ScrapingTasksList(BaseModel):
    channels: list[str] = Field(
        description="List of Telegram channels name in format: 'channel_name'."
    )
    from_date: str = Field(
        description="Date from which to scrape in format: 'DD-MM-YYYY'."
    )