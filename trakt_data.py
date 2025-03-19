import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict, TypeVar, cast

import click
import requests
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

logger = logging.getLogger("trakt-data")

T = TypeVar("T")

_TRAKT_API_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-key": "",
    "trakt-api-version": "2",
    "Authorization": "Bearer [access_token]",
}


_REGISTRY = CollectorRegistry()

_TRAKT_VIP_YEARS = Gauge(
    "trakt_vip_years",
    documentation="Trakt VIP years",
    labelnames=["username"],
    registry=_REGISTRY,
)


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


class ShowIds(TypedDict):
    trakt: int
    slug: str
    tvdb: int
    imdb: str
    tmdb: int
    tvrage: int | None


class Show(TypedDict):
    title: str
    year: int
    ids: ShowIds


class MovieIds(TypedDict):
    trakt: int
    slug: str
    imdb: str
    tmdb: int


class Movie(TypedDict):
    title: str
    year: int
    ids: MovieIds


class Context:
    session: requests.Session
    output_dir: Path

    def __init__(self, session: requests.Session, output_dir: Path) -> None:
        self.session = session
        self.output_dir = output_dir


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, indent=2)
    path.write_text(data + "\n")


def _trakt_session(client_id: str, access_token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(_TRAKT_API_HEADERS)
    session.headers["trakt-api-key"] = client_id
    session.headers["Authorization"] = f"Bearer {access_token}"
    return session


def _fresh(path: Path, max_age: timedelta) -> bool:
    if not path.exists():
        logger.debug("%s doesn't exist", path)
        return False
    else:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        if mtime > datetime.now() - max_age:
            logger.debug("%s last modified %s, still fresh", path, mtime)
            return True
        return False


def _export_user_profile(ctx: Context) -> None:
    output_path = ctx.output_dir / "user" / "profile.json"

    if _fresh(output_path, timedelta(days=7)):
        return

    response = ctx.session.get(
        "https://api.trakt.tv/users/me",
        params={"extended": "vip"},
    )
    response.raise_for_status()
    data = response.json()

    profile = {
        "username": data["username"],
        "name": data["name"],
        "vip": data["vip"],
        "vip_ep": data["vip_ep"],
        "ids": data["ids"],
        "vip_og": data["vip_og"],
        "vip_years": data["vip_years"],
    }
    _write_json(output_path, profile)


def _export_user_stats(ctx: Context) -> None:
    output_path = ctx.output_dir / "user" / "stats.json"

    if _fresh(output_path, timedelta(days=1)):
        return

    response = ctx.session.get("https://api.trakt.tv/users/me/stats")
    response.raise_for_status()
    data = response.json()
    _write_json(output_path, data)


def _export_hidden(
    ctx: Context,
    section: str,
    filename: str,
    expires_in: timedelta,
) -> None:
    output_path = ctx.output_dir / "hidden" / filename

    if _fresh(output_path, expires_in):
        return

    response = ctx.session.get(f"https://api.trakt.tv/users/hidden/{section}")
    response.raise_for_status()
    data = response.json()
    _write_json(output_path, data)


def _export_hidden_calendar(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="calendar",
        filename="hidden-calendar.json",
        expires_in=timedelta(days=1),
    )


def _export_hidden_progress_collected(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="progress_collected",
        filename="hidden-progress-collected.json",
        expires_in=timedelta(days=7),
    )


def _export_hidden_progress_watched_reset(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="progress_watched_reset",
        filename="hidden-progress-watched-reset.json",
        expires_in=timedelta(days=1),
    )


def _export_hidden_progress_watched(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="progress_watched",
        filename="hidden-progress-watched.json",
        expires_in=timedelta(days=1),
    )


def _export_hidden_recommendations(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="recommendations",
        filename="hidden-recommendations.json",
        expires_in=timedelta(days=7),
    )


def _read_json_data(path: Path, return_type: type[T]) -> T:
    return cast(T, json.loads(path.read_text()))


def _generate_metrics(data_path: Path) -> None:
    user_profile = _read_json_data(
        data_path / "user" / "profile.json",
        ExportUserProfile,
    )
    username = user_profile["username"]

    _TRAKT_VIP_YEARS.labels(username=username).set(user_profile["vip_years"])

    metrics_path: str = str(data_path / "metrics.prom")
    write_to_textfile(metrics_path, _REGISTRY)


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

    ctx = Context(
        session=_session,
        output_dir=output_dir,
    )

    _export_hidden_calendar(ctx)
    _export_hidden_progress_collected(ctx)
    _export_hidden_progress_watched_reset(ctx)
    _export_hidden_progress_watched(ctx)
    _export_hidden_recommendations(ctx)
    _export_user_profile(ctx)
    _export_user_stats(ctx)

    _generate_metrics(data_path=output_dir)


if __name__ == "__main__":
    main()
