"""Scrape live RuneScape player counts and persist them to a local database.

Collects the total online player count for both RS3 and Old School
RuneScape, as well as per-world population data for OSRS.  Results are
stored in a SQLite database via SQLAlchemy for later analysis.
"""

import datetime
import re
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

from models import (
    PlayerCount,
    PlayerCountByWorld,
    get_engine,
    get_session,
    init_db,
)

#Reusable session for HTTP connection pooling across requests.
_http = requests.Session()
_http.headers.update({"User-Agent": "Mozilla/5.0"})

#Public endpoint that returns the current RS3 online player count.
_RS3_PLAYER_COUNT_URL = (
    "https://www.runescape.com/c=JBGbhMzTjw4/player_count.js"
    "?varname=iPlayerCount"
    "&callback=jQuery36007308654769072941_1768834964767"
    "&_=1768834964768"
)


_OSRS_WORLDS_URL = "https://oldschool.runescape.com/slu"


def get_combined_player_count():
    """Return the combined RS3 + OSRS player count, or ``None`` on failure.

    The official RS3 endpoint actually reports a combined total for both
    games.  Use alongside :func:`get_osrs_player_count` and subtract to
    derive the true RS3-only figure.
    """
    resp = _http.get(_RS3_PLAYER_COUNT_URL, timeout=20)
    match = re.search(r"\((\d+)\)", resp.text)
    if match:
        return int(match.group(1))
    print("Could not find RS3 player count")
    return None


def get_osrs_player_count():
    """Return the current OSRS online player count, or ``None`` on failure.

    The count is scraped from the Old School RuneScape homepage.
    """
    resp = _http.get(_OSRS_WORLDS_URL, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")
    tag = soup.find("p", class_="player-count")
    if tag:
        number = re.search(r"[\d,]+", tag.text).group().replace(",", "")
        return int(number)
    print("Could not find OSRS player count")
    return None


def players_by_world():
    """Return a DataFrame of OSRS world populations.

    Columns: ``World``, ``Players``, ``Location``, ``Type``, ``Activity``.
    Rows are sorted by player count in descending order.
    """
    resp = _http.get(_OSRS_WORLDS_URL, timeout=20)
    resp.raise_for_status()

    df = pd.read_html(StringIO(resp.text))[0]
    df.columns = ["World", "Players", "Location", "Type", "Activity"]

    df["World"] = df["World"].str.replace(r"^.*?(\d+)$", r"\1", regex=True)
    df["Players"] = (
        df["Players"]
        .str.replace(" players", "")
        .str.replace(",", "")
        .fillna(0)
        .astype(int)
    )
    df["Activity"] = (
        df["Activity"].replace(["-", "", None], "No Activity").fillna("No Activity")
    )

    return df.sort_values(by="Players", ascending=False).reset_index(drop=True)


def main():
    """Scrape player counts and save them to the database."""
    engine = get_engine()
    init_db(engine)
    session = get_session(engine)

    combined_count = get_combined_player_count()
    osrs_count = get_osrs_player_count()
    world_df = players_by_world()

    #The official RS3 endpoint reports RS3 + OSRS combined.
    rs3_count = None
    if combined_count is not None and osrs_count is not None:
        rs3_count = combined_count - osrs_count

    timestamp = datetime.datetime.now()

    if rs3_count is not None:
        session.add(
            PlayerCount(player_count=rs3_count, game="RS3", timestamp=timestamp)
        )
    if osrs_count is not None:
        session.add(
            PlayerCount(player_count=osrs_count, game="OSRS", timestamp=timestamp)
        )

    #Bulk-insert per-world records using to_dict for speed over iterrows.
    session.add_all(
        PlayerCountByWorld(
            world=row["World"],
            players=row["Players"],
            location=row["Location"],
            type=row["Type"],
            activity=row["Activity"],
            timestamp=timestamp,
        )
        for row in world_df.to_dict("records")
    )

    session.commit()
    session.close()

    print(f"Combined (reported): {combined_count:,}" if combined_count else "Combined: N/A")
    print(f"RS3 Players Online:  {rs3_count:,}" if rs3_count is not None else "RS3: N/A")
    print(f"OSRS Players Online: {osrs_count:,}" if osrs_count is not None else "OSRS: N/A")
    print(f"Timestamp: {timestamp:%Y-%m-%d %H:%M:%S}")
    print(world_df)
    print("\nData saved to database.")


if __name__ == "__main__":
    main()
