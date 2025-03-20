import json
import logging
import random
from datetime import datetime, timedelta, timezone
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


class ShowIDs(TypedDict):
    trakt: int
    slug: str
    tvdb: int
    imdb: str
    tmdb: int
    tvrage: int | None


class Show(TypedDict):
    title: str
    year: int
    ids: ShowIDs


class ShowExtended(TypedDict):
    title: str
    year: int
    ids: ShowIDs
    first_aired: str
    runtime: int
    network: str
    country: str
    status: str
    updated_at: str
    language: str
    aired_episodes: int


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
    expired_data_files: set[Path]

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
        expired_data_files: set[Path] = set(),
    ) -> None:
        self.session = session
        self.output_dir = output_dir
        self.expired_data_files = expired_data_files


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


def _fresh(ctx: Context, path: Path) -> bool:
    if not path.exists():
        logger.debug("%s doesn't exist", path)
        return False
    elif path in ctx.expired_data_files:
        logger.debug("%s is expired", path)
        return False
    else:
        return True


def _trakt_api_get(ctx: Context, path: str, params: dict[str, str] = {}) -> Any:
    if not path.startswith("/"):
        path = f"/{path}"
    response = ctx.session.get(f"https://api.trakt.tv{path}", params=params)
    response.raise_for_status()
    return response.json()


def _export_user_profile(ctx: Context) -> None:
    output_path = ctx.output_dir / "user" / "profile.json"

    if _fresh(ctx, output_path):
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

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(ctx, path="/users/me/stats")
    _write_json(output_path, data)


def _export_hidden(
    ctx: Context,
    section: str,
    filename: str,
) -> None:
    output_path = ctx.output_dir / "hidden" / filename

    if _fresh(ctx, output_path):
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

    if _fresh(ctx, output_path):
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

    if not _fresh(ctx, output_path):
        data = _trakt_api_get(ctx, path="/users/me/lists")
        _write_json(output_path, data)

    assert output_path.exists()
    lists = _read_json_data(output_path, list[List])
    _export_lists_list_all(ctx, lists)


def _export_lists_watchlist(ctx: Context) -> None:
    output_path = ctx.output_dir / "lists" / "watchlist.json"

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(
        ctx,
        path="/users/me/watchlist",
        params={"sort_by": "rank", "sort_how": "asc"},
    )
    _write_json(output_path, data)


def _export_media_movie(ctx: Context, trakt_id: int) -> MovieExtended:
    output_path = ctx.output_dir / "media" / "movies" / f"{trakt_id}.json"

    if _fresh(ctx, output_path):
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


def _export_media_show(ctx: Context, trakt_id: int) -> ShowExtended:
    output_path = ctx.output_dir / "media" / "shows" / f"{trakt_id}.json"

    if _fresh(ctx, output_path):
        return _read_json_data(output_path, ShowExtended)

    data = _trakt_api_get(ctx, path=f"/shows/{trakt_id}", params={"extended": "full"})
    show: ShowExtended = {
        "title": data["title"],
        "year": data["year"],
        "ids": data["ids"],
        "first_aired": data["first_aired"],
        "runtime": data["runtime"],
        "network": data["network"],
        "country": data["country"],
        "status": data["status"],
        "updated_at": data["updated_at"],
        "language": data["language"],
        "aired_episodes": data["aired_episodes"],
    }
    _write_json(output_path, show)
    return show


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
            trakt_id = item["show"]["ids"]["trakt"]

            show = _export_media_show(ctx, trakt_id=trakt_id)
            status = show["status"]
            year_str = str(show["year"] or _FUTURE_YEAR)

            _TRAKT_WATCHLIST_COUNT.labels(
                media_type="show",
                status=status,
                year=year_str,
            ).inc()
        else:
            logger.warning("Unknown media type: %s", item["type"])

    metrics_path: str = str(data_path / "metrics.prom")
    write_to_textfile(metrics_path, _REGISTRY)


def _file_updated_at(data_path: Path, filename: Path) -> datetime:
    mtime = datetime.fromtimestamp(filename.stat().st_mtime, tz=timezone.utc)
    updated_at = mtime
    relative_path: str = str(filename.relative_to(data_path))
    if relative_path.startswith("hidden/"):
        items = json.loads(filename.read_text())
        hidden_ats = [datetime.fromisoformat(item["hidden_at"]) for item in items]
        if hidden_ats:
            updated_at = max(hidden_ats)
    elif relative_path == "lists/lists.json":
        items = json.loads(filename.read_text())
        updated_ats = [datetime.fromisoformat(item["updated_at"]) for item in items]
        if updated_ats:
            updated_at = max(updated_ats)
    elif relative_path.startswith("media/"):
        data = json.loads(filename.read_text())
        updated_at = datetime.fromisoformat(data["updated_at"])

    assert updated_at, "updated_at is not set"
    assert updated_at.tzinfo, "updated_at is not offset-aware"
    return updated_at


def _weighted_shuffle(data_path: Path, files: list[Path]) -> list[Path]:
    now = datetime.now(tz=timezone.utc)

    ages: dict[Path, timedelta] = {
        file: now - _file_updated_at(data_path, file) for file in files
    }
    file_weights: dict[Path, float] = {
        file: 1.0 / (1.0 + (ages[file] / timedelta(days=1))) for file in files
    }

    def random_key(file: Path) -> float:
        return float(random.random() ** (1.0 / max(file_weights[file], 0.0001)))

    return sorted(files, key=random_key, reverse=True)


def _compute_expired_data_files(data_path: Path, limit: int) -> set[Path]:
    files = list(data_path.glob("**/*.json"))
    shuffled_files = _weighted_shuffle(data_path, files)
    expired_files = shuffled_files[:limit]

    for file in expired_files:
        logger.debug(
            "Expiring '%s' (modified %s)",
            file,
            _file_updated_at(data_path, file),
        )

    return set(expired_files)


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
    "--expire-limit",
    type=int,
    default=10,
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
    expire_limit: int,
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
        expired_data_files=_compute_expired_data_files(output_dir, limit=expire_limit),
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
