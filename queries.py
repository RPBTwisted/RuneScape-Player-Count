"""Reusable query functions for RuneScape player-count data.

All time-series queries support variable granularity (time bucketing)
and aggregation (average vs peak).
"""

from datetime import datetime
from enum import Enum

from sqlalchemy import Integer as SAInteger, func
from sqlalchemy.orm import Session

from models import PlayerCount, PlayerCountByWorld


class Granularity(str, Enum):
    FIVE_MIN = "5min"
    FIFTEEN_MIN = "15min"
    THIRTY_MIN = "30min"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class Aggregation(str, Enum):
    AVERAGE = "average"
    PEAK = "peak"


# SQLite strftime patterns for time bucketing
_BUCKET_FORMAT = {
    Granularity.FIVE_MIN: None,  # handled specially
    Granularity.FIFTEEN_MIN: None,  # handled specially
    Granularity.THIRTY_MIN: None,  # handled specially
    Granularity.HOURLY: "%Y-%m-%d %H:00:00",
    Granularity.DAILY: "%Y-%m-%d",
    Granularity.WEEKLY: "%Y-%W",
    Granularity.MONTHLY: "%Y-%m",
}


def _time_bucket(timestamp_col, granularity: Granularity):
    """Return a SQL expression that truncates *timestamp_col* to the bucket."""
    fmt = _BUCKET_FORMAT[granularity]
    if fmt is not None:
        return func.strftime(fmt, timestamp_col)

    # Sub-hourly: truncate minutes to nearest N-minute boundary
    minutes = {
        Granularity.FIVE_MIN: 5,
        Granularity.FIFTEEN_MIN: 15,
        Granularity.THIRTY_MIN: 30,
    }[granularity]

    return func.strftime(
        "%Y-%m-%d %H:",
        timestamp_col,
    ) + func.printf(
        "%02d:00",
        (func.cast(func.strftime("%M", timestamp_col), SAInteger) / minutes) * minutes,
    )


def _agg_func(agg: Aggregation):
    return func.avg if agg == Aggregation.AVERAGE else func.max


def player_count_timeseries(
    session: Session,
    game: str,
    start: datetime,
    end: datetime,
    granularity: Granularity = Granularity.HOURLY,
    agg: Aggregation = Aggregation.AVERAGE,
):
    """Total player count over time for a single game (RS3 or OSRS)."""
    bucket = _time_bucket(PlayerCount.timestamp, granularity)
    agg_fn = _agg_func(agg)

    return (
        session.query(
            bucket.label("time_bucket"),
            agg_fn(PlayerCount.player_count).label("player_count"),
        )
        .filter(
            PlayerCount.game == game,
            PlayerCount.timestamp >= start,
            PlayerCount.timestamp <= end,
        )
        .group_by(bucket)
        .order_by(bucket)
        .all()
    )


def combined_total_timeseries(
    session: Session,
    start: datetime,
    end: datetime,
    granularity: Granularity = Granularity.HOURLY,
    agg: Aggregation = Aggregation.AVERAGE,
):
    """Combined RS3 + OSRS player count over time."""
    bucket = _time_bucket(PlayerCount.timestamp, granularity)
    agg_fn = _agg_func(agg)

    return (
        session.query(
            bucket.label("time_bucket"),
            agg_fn(PlayerCount.player_count).label("player_count"),
        )
        .filter(
            PlayerCount.timestamp >= start,
            PlayerCount.timestamp <= end,
        )
        .group_by(bucket)
        .order_by(bucket)
        .all()
    )


def player_count_by_type(
    session: Session,
    start: datetime,
    end: datetime,
    granularity: Granularity = Granularity.HOURLY,
    agg: Aggregation = Aggregation.AVERAGE,
):
    """F2P vs Members player count over time (from world data).

    Groups worlds by type within each time bucket, summing their players.
    """
    bucket = _time_bucket(PlayerCountByWorld.timestamp, granularity)

    return (
        session.query(
            bucket.label("time_bucket"),
            PlayerCountByWorld.type.label("player_type"),
            func.sum(PlayerCountByWorld.players).label("player_count"),
        )
        .filter(
            PlayerCountByWorld.timestamp >= start,
            PlayerCountByWorld.timestamp <= end,
        )
        .group_by(bucket, PlayerCountByWorld.type)
        .order_by(bucket)
        .all()
    )


def player_count_by_region(
    session: Session,
    start: datetime,
    end: datetime,
    granularity: Granularity = Granularity.HOURLY,
    agg: Aggregation = Aggregation.AVERAGE,
):
    """Player count grouped by server region over time."""
    bucket = _time_bucket(PlayerCountByWorld.timestamp, granularity)

    return (
        session.query(
            bucket.label("time_bucket"),
            PlayerCountByWorld.location.label("region"),
            func.sum(PlayerCountByWorld.players).label("player_count"),
        )
        .filter(
            PlayerCountByWorld.timestamp >= start,
            PlayerCountByWorld.timestamp <= end,
        )
        .group_by(bucket, PlayerCountByWorld.location)
        .order_by(bucket)
        .all()
    )


def player_count_by_world(
    session: Session,
    world: str,
    start: datetime,
    end: datetime,
    granularity: Granularity = Granularity.HOURLY,
    agg: Aggregation = Aggregation.AVERAGE,
):
    """Player count for a specific world over time."""
    bucket = _time_bucket(PlayerCountByWorld.timestamp, granularity)
    agg_fn = _agg_func(agg)

    return (
        session.query(
            bucket.label("time_bucket"),
            agg_fn(PlayerCountByWorld.players).label("player_count"),
        )
        .filter(
            PlayerCountByWorld.world == world,
            PlayerCountByWorld.timestamp >= start,
            PlayerCountByWorld.timestamp <= end,
        )
        .group_by(bucket)
        .order_by(bucket)
        .all()
    )


def world_snapshot(session: Session, timestamp: datetime | None = None):
    """Per-world population at a specific point in time (default: latest)."""
    if timestamp is None:
        timestamp = session.query(
            func.max(PlayerCountByWorld.timestamp)
        ).scalar()
        if timestamp is None:
            return []

    return (
        session.query(
            PlayerCountByWorld.world,
            PlayerCountByWorld.players,
            PlayerCountByWorld.location,
            PlayerCountByWorld.type,
            PlayerCountByWorld.activity,
            PlayerCountByWorld.timestamp,
        )
        .filter(PlayerCountByWorld.timestamp == timestamp)
        .order_by(PlayerCountByWorld.players.desc())
        .all()
    )


def player_count_by_activity(
    session: Session,
    start: datetime,
    end: datetime,
):
    """Total players grouped by world activity over a date range."""
    return (
        session.query(
            PlayerCountByWorld.activity,
            func.sum(PlayerCountByWorld.players).label("total_players"),
            func.count(PlayerCountByWorld.id).label("snapshot_count"),
        )
        .filter(
            PlayerCountByWorld.timestamp >= start,
            PlayerCountByWorld.timestamp <= end,
        )
        .group_by(PlayerCountByWorld.activity)
        .order_by(func.sum(PlayerCountByWorld.players).desc())
        .all()
    )
