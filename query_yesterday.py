"""Query yesterday's OSRS world-population snapshot from the local database.

Prints total, free-to-play, and members player counts from the most
recent snapshot recorded on the previous calendar day.
"""

from datetime import datetime, timedelta

from sqlalchemy import case, func

from models import PlayerCountByWorld, get_engine, get_session


def main():
    """Load and display yesterday's aggregated player counts."""
    engine = get_engine()
    session = get_session(engine)

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    start = datetime.combine(yesterday, datetime.min.time())
    end = datetime.combine(today, datetime.min.time())

    #Find the latest snapshot timestamp recorded yesterday.
    latest_ts = (
        session.query(func.max(PlayerCountByWorld.timestamp))
        .filter(PlayerCountByWorld.timestamp >= start)
        .filter(PlayerCountByWorld.timestamp < end)
        .scalar()
    )

    if latest_ts is None:
        print(f"No data found for {yesterday}")
        return

    #Aggregate total, free, and members counts in a single query.
    total, free, members = (
        session.query(
            func.sum(PlayerCountByWorld.players).label("total_players"),
            func.sum(
                case(
                    (PlayerCountByWorld.type == "Free", PlayerCountByWorld.players),
                    else_=0,
                )
            ).label("free_players"),
            func.sum(
                case(
                    (PlayerCountByWorld.type == "Members", PlayerCountByWorld.players),
                    else_=0,
                )
            ).label("members_players"),
        )
        .filter(PlayerCountByWorld.timestamp == latest_ts)
        .one()
    )

    print(f"Last snapshot for {yesterday} ({latest_ts}):")
    print(f"  Total players:   {total}")
    print(f"  Free players:    {free}")
    print(f"  Members players: {members}")


if __name__ == "__main__":
    main()
