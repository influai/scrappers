import json
import os

from database.database import Base
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


# Create SQLA engine and session
# ! Note that database credentials are set as env variables at docker-compose
url: str = f"postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_IP")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}"
# Need to change search path because i don't have permission in public schema
engine: Engine = create_engine(
    url,
    connect_args={"options": "-csearch_path={}".format("channels_data")},
    json_serializer=lambda x: json.dumps(x, ensure_ascii=False),
)
session = sessionmaker(bind=engine)


def get_session():
    """
    DB connection generator that yields a new session each time it's called.
    """
    while True:
        connection: Session = session()
        try:
            yield connection
        finally:
            connection.close()


def init_db():
    """Create all tables in the database based on the data models."""
    Base.metadata.create_all(engine, checkfirst=True)


if __name__ == "__main__":
    init_db()
