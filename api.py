"""FastAPI application exposing RuneScape player-count data."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import get_engine, get_session, init_db
from queries import (
    Aggregation,
    Granularity,
    combined_total_timeseries,
    player_count_by_activity,
    player_count_by_region,
    player_count_by_type,
    player_count_by_world,
    player_count_timeseries,
    world_snapshot,
)

app = FastAPI(title="RuneScape Player Count API")

engine = get_engine()
init_db(engine)


def get_db():
    session = get_session(engine)
    try:
        yield session
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class TimeSeriesPoint(BaseModel):
    time_bucket: str
    player_count: float


class TypedTimeSeriesPoint(BaseModel):
    time_bucket: str
    player_type: str
    player_count: float


class RegionTimeSeriesPoint(BaseModel):
    time_bucket: str
    region: str
    player_count: float


class WorldSnapshotEntry(BaseModel):
    world: str
    players: int
    location: Optional[str]
    type: Optional[str]
    activity: Optional[str]
    timestamp: datetime


class ActivityEntry(BaseModel):
    activity: Optional[str]
    total_players: int
    snapshot_count: int


# ---------------------------------------------------------------------------
# Default date helpers
# ---------------------------------------------------------------------------


def _default_start() -> datetime:
    return datetime.now() - timedelta(hours=24)


def _default_end() -> datetime:
    return datetime.now()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/player-count", response_model=list[TimeSeriesPoint])
def get_player_count(
    game: str = Query("OSRS", description="Game: OSRS or RS3"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    granularity: Granularity = Query(Granularity.HOURLY),
    agg: Aggregation = Query(Aggregation.AVERAGE),
    db: Session = Depends(get_db),
):
    rows = player_count_timeseries(
        db,
        game=game,
        start=start or _default_start(),
        end=end or _default_end(),
        granularity=granularity,
        agg=agg,
    )
    return [{"time_bucket": r.time_bucket, "player_count": r.player_count} for r in rows]


@app.get("/api/player-count/combined", response_model=list[TimeSeriesPoint])
def get_combined_total(
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    granularity: Granularity = Query(Granularity.HOURLY),
    agg: Aggregation = Query(Aggregation.AVERAGE),
    db: Session = Depends(get_db),
):
    rows = combined_total_timeseries(
        db,
        start=start or _default_start(),
        end=end or _default_end(),
        granularity=granularity,
        agg=agg,
    )
    return [{"time_bucket": r.time_bucket, "player_count": r.player_count} for r in rows]


@app.get("/api/player-count/by-type", response_model=list[TypedTimeSeriesPoint])
def get_by_type(
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    granularity: Granularity = Query(Granularity.HOURLY),
    agg: Aggregation = Query(Aggregation.AVERAGE),
    db: Session = Depends(get_db),
):
    rows = player_count_by_type(
        db,
        start=start or _default_start(),
        end=end or _default_end(),
        granularity=granularity,
        agg=agg,
    )
    return [
        {"time_bucket": r.time_bucket, "player_type": r.player_type, "player_count": r.player_count}
        for r in rows
    ]


@app.get("/api/player-count/by-region", response_model=list[RegionTimeSeriesPoint])
def get_by_region(
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    granularity: Granularity = Query(Granularity.HOURLY),
    agg: Aggregation = Query(Aggregation.AVERAGE),
    db: Session = Depends(get_db),
):
    rows = player_count_by_region(
        db,
        start=start or _default_start(),
        end=end or _default_end(),
        granularity=granularity,
        agg=agg,
    )
    return [
        {"time_bucket": r.time_bucket, "region": r.region, "player_count": r.player_count}
        for r in rows
    ]


@app.get("/api/player-count/by-world/{world}", response_model=list[TimeSeriesPoint])
def get_by_world(
    world: str,
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    granularity: Granularity = Query(Granularity.HOURLY),
    agg: Aggregation = Query(Aggregation.AVERAGE),
    db: Session = Depends(get_db),
):
    rows = player_count_by_world(
        db,
        world=world,
        start=start or _default_start(),
        end=end or _default_end(),
        granularity=granularity,
        agg=agg,
    )
    return [{"time_bucket": r.time_bucket, "player_count": r.player_count} for r in rows]


@app.get("/api/player-count/by-activity", response_model=list[ActivityEntry])
def get_by_activity(
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    db: Session = Depends(get_db),
):
    rows = player_count_by_activity(
        db,
        start=start or _default_start(),
        end=end or _default_end(),
    )
    return [
        {"activity": r.activity, "total_players": r.total_players, "snapshot_count": r.snapshot_count}
        for r in rows
    ]


@app.get("/api/worlds/snapshot", response_model=list[WorldSnapshotEntry])
def get_world_snapshot(
    timestamp: Optional[datetime] = Query(None),
    db: Session = Depends(get_db),
):
    rows = world_snapshot(db, timestamp=timestamp)
    return [
        {
            "world": r.world,
            "players": r.players,
            "location": r.location,
            "type": r.type,
            "activity": r.activity,
            "timestamp": r.timestamp,
        }
        for r in rows
    ]
