import os
import json

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .database import Base


# Load db creds from .env
load_dotenv()
# Check loaded envs
REQUIRED_ENV_VARS = [
    "DB_IP",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
]
missing_vars = [var for var in REQUIRED_ENV_VARS if os.getenv(var) is None]
if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

# Create SQLA engine and session
url = f"postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_IP")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}"
# Need to change search path because i don't have permission in public schema
engine = create_engine(
    url,
    connect_args={'options': '-csearch_path={}'.format("channels_data")},
    json_serializer=lambda x: json.dumps(x, ensure_ascii=False))
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

# Create all tables in the database based on the models
def init_db():
    Base.metadata.create_all(engine)
