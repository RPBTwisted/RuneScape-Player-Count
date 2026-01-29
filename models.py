"""SQLAlchemy models and helpers for the RuneScape player-stats database.

Defines two tables:

* **playercount** -- total online players per game (RS3 / OSRS).
* **playercountbyworld** -- per-world population snapshots for OSRS.
"""

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class PlayerCount(Base):
    """A single total-player-count observation for one game."""

    __tablename__ = "playercount"

    id = Column(Integer, primary_key=True)
    player_count = Column(Integer, nullable=False)
    game = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return (
            f"<PlayerCount(game={self.game!r}, "
            f"count={self.player_count}, "
            f"timestamp={self.timestamp!r})>"
        )


class PlayerCountByWorld(Base):
    """A per-world population snapshot for OSRS."""

    __tablename__ = "playercountbyworld"

    id = Column(Integer, primary_key=True)
    world = Column(String, nullable=False)
    players = Column(Integer, nullable=False)
    location = Column(String)
    type = Column(String)
    activity = Column(String)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return (
            f"<PlayerCountByWorld(world={self.world!r}, "
            f"players={self.players})>"
        )


def get_engine(db_url="sqlite:///runescape_stats.db"):
    """Create and return a SQLAlchemy engine.

    Args:
        db_url: Database connection string. Defaults to a local SQLite file.
    """
    return create_engine(db_url)


def init_db(engine):
    """Create all tables that do not yet exist."""
    Base.metadata.create_all(engine)


def get_session(engine):
    """Return a new SQLAlchemy session bound to *engine*."""
    return sessionmaker(bind=engine)()
