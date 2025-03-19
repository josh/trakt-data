import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict, TypeVar, cast

import click
import requests
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

logger = logging.getLogger("trakt-data")


@click.command()
@click.option(
    "--trakt-client-id",
    required=True,
    envvar="TRAKT_CLIENT_ID",
)
@click.option(
    "--trakt-access-token",
    required=True,
    envvar="TRAKT_ACCESS_TOKEN",
)
@click.option(
    "--output-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    envvar="OUTPUT_DIR",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
def main(
    trakt_client_id: str,
    trakt_access_token: str,
    output_dir: Path,
    verbose: bool,
) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    _session = _trakt_session(
        client_id=trakt_client_id,
        access_token=trakt_access_token,
    )

    user_profile_path = output_dir / "user" / "profile.json"
    profile = _export_user_profile(_session, output_path=user_profile_path)
    _write_json(user_profile_path, profile)

    user_stats_path = output_dir / "user" / "stats.json"
    stats = _export_user_stats(_session, output_path=user_stats_path)
    _write_json(user_stats_path, stats)

    _generate_metrics(data_path=output_dir)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, indent=2)
    path.write_text(data + "\n")


_TRAKT_API_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-key": "",
    "trakt-api-version": "2",
    "Authorization": "Bearer [access_token]",
}


def _trakt_session(client_id: str, access_token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(_TRAKT_API_HEADERS)
    session.headers["trakt-api-key"] = client_id
    session.headers["Authorization"] = f"Bearer {access_token}"
    return session


class UserIDs(TypedDict):
    slug: str


class ExportUserProfile(TypedDict):
    username: str
    name: str
    vip: bool
    vip_ep: bool
    ids: UserIDs
    vip_og: bool
    vip_years: int


def _export_user_profile(
    session: requests.Session,
    output_path: Path,
) -> ExportUserProfile:
    user_profile, mtime = _read_json_mtime_data(output_path, ExportUserProfile)

    if mtime and user_profile and mtime > datetime.now() - timedelta(days=7):
        logger.debug("%s mtime is %s, still fresh", output_path, mtime)
        return user_profile

    response = session.get("https://api.trakt.tv/users/me", params={"extended": "vip"})
    response.raise_for_status()
    data = response.json()

    return {
        "username": data["username"],
        "name": data["name"],
        "vip": data["vip"],
        "vip_ep": data["vip_ep"],
        "ids": data["ids"],
        "vip_og": data["vip_og"],
        "vip_years": data["vip_years"],
    }


class ExportUserMoviesStats(TypedDict):
    plays: int
    watched: int
    minutes: int
    collected: int
    ratings: int
    comments: int


class ExportUserShowsStats(TypedDict):
    watched: int
    collected: int
    ratings: int
    comments: int


class ExportUserSeasonsStats(TypedDict):
    ratings: int
    comments: int


class ExportUserEpisodesStats(TypedDict):
    plays: int
    watched: int
    minutes: int
    collected: int
    ratings: int
    comments: int


class ExportUserNetworkStats(TypedDict):
    friends: int
    followers: int
    following: int


ExportUserRatingsDistribution = TypedDict(
    "ExportUserRatingsDistribution",
    {
        "1": int,
        "2": int,
        "3": int,
        "4": int,
        "5": int,
        "6": int,
        "7": int,
        "8": int,
        "9": int,
        "10": int,
    },
)


class ExportUserRatingsStats(TypedDict):
    total: int
    distribution: ExportUserRatingsDistribution


class ExportUserStats(TypedDict):
    movies: ExportUserMoviesStats
    shows: ExportUserShowsStats
    seasons: ExportUserSeasonsStats
    episodes: ExportUserEpisodesStats
    network: ExportUserNetworkStats
    ratings: ExportUserRatingsStats


def _export_user_stats(
    session: requests.Session,
    output_path: Path,
) -> ExportUserStats:
    user_stats, mtime = _read_json_mtime_data(output_path, ExportUserStats)

    if mtime and user_stats and mtime > datetime.now() - timedelta(days=1):
        logger.debug("%s mtime is %s, still fresh", output_path, mtime)
        return user_stats

    response = session.get("https://api.trakt.tv/users/me/stats")
    response.raise_for_status()
    data = response.json()

    return {
        "movies": data["movies"],
        "shows": data["shows"],
        "seasons": data["seasons"],
        "episodes": data["episodes"],
        "network": data["network"],
        "ratings": data["ratings"],
    }


T = TypeVar("T")


def _read_json_mtime_data(
    path: Path,
    return_type: type[T],
) -> tuple[T, datetime] | tuple[None, None]:
    if not path.exists():
        return None, None
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    obj = json.loads(path.read_text())
    return cast(T, obj), mtime


def _read_json_data(path: Path, return_type: type[T]) -> T:
    return cast(T, json.loads(path.read_text()))


registry = CollectorRegistry()

trakt_vip_years = Gauge(
    "trakt_vip_years",
    documentation="Trakt VIP years",
    labelnames=["username"],
    registry=registry,
)


def _generate_metrics(data_path: Path) -> None:
    user_profile = _read_json_data(
        data_path / "user" / "profile.json",
        ExportUserProfile,
    )
    username = user_profile["username"]

    trakt_vip_years.labels(username=username).set(user_profile["vip_years"])

    metrics_path: str = str(data_path / "metrics.prom")
    write_to_textfile(metrics_path, registry)


if __name__ == "__main__":
    main()
