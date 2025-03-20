import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypedDict, TypeVar, cast

import click
import requests
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

logger = logging.getLogger("trakt-data")

T = TypeVar("T")

_FUTURE_YEAR = 3000

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

_TRAKT_WATCHLIST_COUNT = Gauge(
    "trakt_watchlist_count",
    documentation="Number of items in Trakt watchlist",
    labelnames=["media_type", "status", "year"],
    registry=_REGISTRY,
)

_TRAKT_WATCHLIST_RUNTIME = Gauge(
    "trakt_watchlist_minutes",
    documentation="Number of minutes in Trakt watchlist",
    labelnames=["media_type", "status", "year"],
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


class MovieIDs(TypedDict):
    trakt: int
    slug: str
    imdb: str
    tmdb: int


class Movie(TypedDict):
    title: str
    year: int
    ids: MovieIDs


class MovieExtended(TypedDict):
    title: str
    year: int
    ids: MovieIDs
    released: str
    runtime: int
    country: str
    status: str
    updated_at: str
    language: str


class ListIDs(TypedDict):
    trakt: int
    slug: str


class List(TypedDict):
    ids: ListIDs


class ListMovie(TypedDict):
    rank: int
    id: int
    listed_at: str
    notes: str | None
    type: Literal["movie"]
    movie: Movie


class ListShow(TypedDict):
    rank: int
    id: int
    listed_at: str
    notes: str | None
    type: Literal["show"]
    show: Show


ListItem = ListMovie | ListShow


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


def _max_age(last_modified: datetime) -> timedelta:
    """
    Calculates dynamic expiration time using a continuous curve based on file age.
    Uses square root formula to gradually increase check intervals from 15 minutes (recent files) to 7 days (old files).
    Formula: base_interval * (1 + (2.5 * (age_hours / 24)) ** 0.5), capped at 168 hours.
    Provides smooth transition rather than discrete steps for optimal refresh scheduling.
    """
    now = datetime.now()
    age = now - last_modified
    age_hours = age.total_seconds() / 3600
    base_interval = 0.25
    if age_hours <= 0:
        hours_until_next_check = base_interval
    else:
        hours_until_next_check = base_interval * (1 + (2.5 * (age_hours / 24)) ** 0.5)
    hours_until_next_check = min(hours_until_next_check, 168)
    return timedelta(hours=hours_until_next_check)


def _fresh(path: Path) -> bool:
    if not path.exists():
        logger.debug("%s doesn't exist", path)
        return False
    else:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        max_age: timedelta = _max_age(mtime)
        expires_at: datetime = mtime + max_age
        expires_in: timedelta = max(timedelta(0), expires_at - datetime.now())
        if expires_at > datetime.now():
            logger.debug(
                "%s: last modified %s, expires in %s, fresh", path, mtime, expires_in
            )
            return True
        else:
            logger.debug(
                "%s: last modified %s, expires in %s, stale", path, mtime, expires_in
            )
            return False


def _trakt_api_get(ctx: Context, path: str, params: dict[str, str] = {}) -> Any:
    if not path.startswith("/"):
        path = f"/{path}"
    response = ctx.session.get(f"https://api.trakt.tv{path}", params=params)
    response.raise_for_status()
    return response.json()


def _export_user_profile(ctx: Context) -> None:
    output_path = ctx.output_dir / "user" / "profile.json"

    if _fresh(output_path):
        return

    data = _trakt_api_get(ctx, path="/users/me", params={"extended": "vip"})

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

    if _fresh(output_path):
        return

    data = _trakt_api_get(ctx, path="/users/me/stats")
    _write_json(output_path, data)


def _export_hidden(
    ctx: Context,
    section: str,
    filename: str,
) -> None:
    output_path = ctx.output_dir / "hidden" / filename

    if _fresh(output_path):
        return

    data = _trakt_api_get(ctx, path=f"/users/hidden/{section}")
    _write_json(output_path, data)


def _export_hidden_calendar(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="calendar",
        filename="hidden-calendar.json",
    )


def _export_hidden_progress_collected(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="progress_collected",
        filename="hidden-progress-collected.json",
    )


def _export_hidden_progress_watched_reset(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="progress_watched_reset",
        filename="hidden-progress-watched-reset.json",
    )


def _export_hidden_progress_watched(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="progress_watched",
        filename="hidden-progress-watched.json",
    )


def _export_hidden_recommendations(ctx: Context) -> None:
    _export_hidden(
        ctx,
        section="recommendations",
        filename="hidden-recommendations.json",
    )


def _read_json_data(path: Path, return_type: type[T]) -> T:
    return cast(T, json.loads(path.read_text()))


def _export_lists_list(ctx: Context, list_id: int, list_slug: str) -> None:
    output_path = ctx.output_dir / "lists" / f"list-{list_id}-{list_slug}.json"

    if _fresh(output_path):
        return

    data = _trakt_api_get(ctx, path=f"/users/me/lists/{list_id}/items")
    _write_json(output_path, data)


def _export_lists_list_all(ctx: Context, lists: list[List]) -> None:
    list_ids: set[int] = set()

    for lst in lists:
        trakt_id: int = lst["ids"]["trakt"]
        trakt_slug: str = lst["ids"]["slug"]
        _export_lists_list(ctx, trakt_id, trakt_slug)
        list_ids.add(trakt_id)

    for path in ctx.output_dir.glob("lists/list-*.json"):
        list_id = int(path.name.split("-")[1])
        if list_id not in list_ids:
            logger.info(f"Deleting old list: {path}")
            path.unlink()


def _export_lists_lists(ctx: Context) -> None:
    output_path = ctx.output_dir / "lists" / "lists.json"

    if not _fresh(output_path):
        data = _trakt_api_get(ctx, path="/users/me/lists")
        _write_json(output_path, data)

    assert output_path.exists()
    lists = _read_json_data(output_path, list[List])
    _export_lists_list_all(ctx, lists)


def _export_lists_watchlist(ctx: Context) -> None:
    output_path = ctx.output_dir / "lists" / "watchlist.json"

    if _fresh(output_path):
        return

    data = _trakt_api_get(
        ctx,
        path="/users/me/watchlist",
        params={"sort_by": "rank", "sort_how": "asc"},
    )
    _write_json(output_path, data)


def _export_media_movie(ctx: Context, trakt_id: int) -> MovieExtended:
    output_path = ctx.output_dir / "media" / "movies" / f"{trakt_id}.json"

    if _fresh(output_path):
        return _read_json_data(output_path, MovieExtended)

    data = _trakt_api_get(ctx, path=f"/movies/{trakt_id}", params={"extended": "full"})
    movie: MovieExtended = {
        "title": data["title"],
        "year": data["year"],
        "ids": data["ids"],
        "released": data["released"],
        "runtime": data["runtime"],
        "country": data["country"],
        "status": data["status"],
        "updated_at": data["updated_at"],
        "language": data["language"],
    }
    _write_json(output_path, movie)
    return movie


def _generate_metrics(ctx: Context, data_path: Path) -> None:
    user_profile = _read_json_data(
        data_path / "user" / "profile.json",
        ExportUserProfile,
    )
    username = user_profile["username"]

    _TRAKT_VIP_YEARS.labels(username=username).set(user_profile["vip_years"])

    watchlist = _read_json_data(data_path / "lists" / "watchlist.json", list[ListItem])
    for item in watchlist:
        if item["type"] == "movie":
            trakt_id = item["movie"]["ids"]["trakt"]

            movie = _export_media_movie(ctx, trakt_id=trakt_id)
            status = movie["status"]
            year_str = str(movie["year"] or _FUTURE_YEAR)
            runtime = movie["runtime"]

            _TRAKT_WATCHLIST_COUNT.labels(
                media_type="movie",
                status=status,
                year=year_str,
            ).inc()
            _TRAKT_WATCHLIST_RUNTIME.labels(
                media_type="movie",
                status=status,
                year=year_str,
            ).inc(runtime)
        elif item["type"] == "show":
            _TRAKT_WATCHLIST_COUNT.labels(
                media_type="show",
                status="unknown",
                year=_FUTURE_YEAR,
            ).inc()
            _TRAKT_WATCHLIST_RUNTIME.labels(
                media_type="show",
                status="unknown",
                year=_FUTURE_YEAR,
            ).inc(60)
        else:
            logger.warning("Unknown media type: %s", item["type"])

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
    _export_lists_lists(ctx)
    _export_lists_watchlist(ctx)
    _export_user_profile(ctx)
    _export_user_stats(ctx)

    _generate_metrics(ctx, data_path=output_dir)


if __name__ == "__main__":
    main()
