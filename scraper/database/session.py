import json
import os
from contextlib import contextmanager

from database.database import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


dbschema='parse'
engine = create_engine(
    f"postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_IP")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}",
    connect_args={
        "options": f"-csearch_path={dbschema}"
        # The parameters below have been proposed to try to eliminate `dead` sessions for scrapers
        + " -c statement_timeout=100s"
        + " -c lock_timeout=100s"
        + " -c idle_in_transaction_session_timeout=100s"
        + " -c idle_session_timeout=100s"
    },
    json_serializer=lambda x: json.dumps(x, ensure_ascii=False),
    pool_pre_ping=True,
    pool_recycle=100,  # prevent the pool from using a particular connection that has passed a certain age (in sec)
)

Session = sessionmaker(engine)


@contextmanager
def get_database_session():
    """
    Provide a transactional scope around a series of operations.
    """
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """
    Create all tables in the database based on the data models.
    """
    Base.metadata.create_all(engine, checkfirst=True)


if __name__ == "__main__":
    init_db()
