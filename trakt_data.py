import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, TypedDict, TypeVar, cast

import click
import requests
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("trakt-data")

T = TypeVar("T")

_FUTURE_YEAR = 3000
_NOW: float = time.time()

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

_TRAKT_COLLECTION_COUNT = Gauge(
    "trakt_collection_count",
    documentation="Number of items in Trakt collection",
    labelnames=["media_type", "year"],
    registry=_REGISTRY,
)

_TRAKT_RATINGS_COUNT = Gauge(
    "trakt_ratings_count",
    documentation="Number of items in Trakt ratings",
    labelnames=["media_type", "year", "rating"],
    registry=_REGISTRY,
)

_TRAKT_WATCHED_COUNT = Gauge(
    "trakt_watched_count",
    documentation="Number of items in Trakt watched",
    labelnames=["media_type", "year"],
    registry=_REGISTRY,
)

_TRAKT_WATCHED_RUNTIME = Gauge(
    "trakt_watched_runtime",
    documentation="Number of minutes in Trakt watched",
    labelnames=["media_type", "year"],
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


class ExportLastActivities(TypedDict):
    all: str
    movies: "ExportMoviesLastActivities"
    episodes: "ExportEpisodesLastActivities"
    shows: "ExportShowsLastActivities"
    seasons: "ExportSeasonsLastActivities"
    comments: "ExportCommentsLastActivities"
    lists: "ExportListsLastActivities"
    watchlist: "ExportWatchlistLastActivities"
    favorites: "ExportFavoritesLastActivities"
    recommendations: "ExportRecommendationsLastActivities"
    collaborations: "ExportCollaborationsLastActivities"
    account: "ExportAccountLastActivities"
    saved_filters: "ExportSavedFiltersLastActivities"
    notes: "ExportNotesLastActivities"


class ExportMoviesLastActivities(TypedDict):
    watched_at: str
    collected_at: str
    rated_at: str
    watchlisted_at: str
    favorited_at: str
    recommendations_at: str
    commented_at: str
    paused_at: str
    hidden_at: str


class ExportEpisodesLastActivities(TypedDict):
    watched_at: str
    collected_at: str
    rated_at: str
    watchlisted_at: str
    commented_at: str
    paused_at: str


class ExportShowsLastActivities(TypedDict):
    rated_at: str
    watchlisted_at: str
    favorited_at: str
    recommendations_at: str
    commented_at: str
    hidden_at: str
    dropped_at: str


class ExportSeasonsLastActivities(TypedDict):
    rated_at: str
    watchlisted_at: str
    commented_at: str
    hidden_at: str


class ExportCommentsLastActivities(TypedDict):
    liked_at: str
    blocked_at: str


class ExportListsLastActivities(TypedDict):
    liked_at: str
    updated_at: str
    commented_at: str


class ExportWatchlistLastActivities(TypedDict):
    updated_at: str


class ExportFavoritesLastActivities(TypedDict):
    updated_at: str


class ExportRecommendationsLastActivities(TypedDict):
    updated_at: str


class ExportCollaborationsLastActivities(TypedDict):
    updated_at: str


class ExportAccountLastActivities(TypedDict):
    settings_at: str
    followed_at: str
    following_at: str
    pending_at: str
    requested_at: str


class ExportSavedFiltersLastActivities(TypedDict):
    updated_at: str


class ExportNotesLastActivities(TypedDict):
    updated_at: str


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


class MovieIDs(TypedDict):
    trakt: int
    slug: str
    imdb: str
    tmdb: int


class ShowIDs(TypedDict):
    trakt: int
    slug: str
    tvdb: int
    imdb: str
    tmdb: int
    tvrage: int | None


class SeasonIDs(TypedDict):
    trakt: int
    tvdb: int
    tmdb: int


class EpisodeIDs(TypedDict):
    trakt: int
    tvdb: int
    imdb: str | None
    tmdb: int
    tvrage: int | None


class Show(TypedDict):
    title: str
    year: int
    ids: ShowIDs


class Season(TypedDict):
    number: int
    ids: SeasonIDs


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
    # Non-standard fields
    seasons: list[Season]


class EpisodeExtended(TypedDict):
    season: int
    number: int
    title: str
    ids: EpisodeIDs
    first_aired: str
    updated_at: str
    runtime: int
    episode_type: str


class Episode(TypedDict):
    season: int
    number: int
    title: str
    ids: EpisodeIDs


class SeasonExtended(TypedDict):
    number: int
    ids: SeasonIDs
    first_aired: str
    updated_at: str
    # Non-standard fields
    episodes: list[Episode]


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


class CollectedMovie(TypedDict):
    collected_at: str
    updated_at: str
    movie: Movie


class CollectedShow(TypedDict):
    last_collected_at: str
    last_updated_at: str
    show: Show
    seasons: list["CollectedSeason"]


class CollectedSeason(TypedDict):
    number: int
    episodes: list["CollectedEpisode"]


class CollectedEpisode(TypedDict):
    number: int
    collected_at: str


class HistoryMovieItem(TypedDict):
    id: int
    watched_at: str
    action: Literal["scrobble", "checkin", "watch"]
    type: Literal["movie"]
    movie: Movie


class HistoryEpisodeItem(TypedDict):
    id: int
    watched_at: str
    action: Literal["scrobble", "checkin", "watch"]
    type: Literal["episode"]
    episode: Episode
    show: Show


HistoryItem = HistoryMovieItem | HistoryEpisodeItem


class EpisodeRating(TypedDict):
    rated_at: str
    rating: int
    type: Literal["episode"]
    episode: Episode
    show: Show


class MovieRating(TypedDict):
    rated_at: str
    rating: int
    type: Literal["movie"]
    movie: Movie


class ShowRating(TypedDict):
    rated_at: str
    rating: int
    type: Literal["show"]
    show: Show


class WatchedMovie(TypedDict):
    plays: int
    last_watched_at: str
    last_updated_at: str
    movie: Movie


class WatchedShow(TypedDict):
    plays: int
    last_watched_at: str
    last_updated_at: str
    reset_at: str | None
    show: Show
    # seasons: list["WatchedSeason"]


class Context:
    session: requests.Session
    output_dir: Path

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
    ) -> None:
        self.session = session
        self.output_dir = output_dir


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


def _default_cache_dir() -> Path:
    return _xdg_cache_home() / "trakt-data"


class ExportContext(Context):
    exclude_paths: list[Path]
    fresh_paths: list[Path]
    stale_paths: list[Path]

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
        exclude_paths: list[Path],
        fresh_paths: list[Path],
        stale_paths: list[Path],
    ) -> None:
        self.exclude_paths = exclude_paths
        self.fresh_paths = fresh_paths
        self.stale_paths = stale_paths
        super().__init__(session, output_dir)


class MetricsContext(Context):
    cache_dir: Path

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
        cache_dir: Path,
    ) -> None:
        self.cache_dir = cache_dir
        super().__init__(session, output_dir)


def _write_json(path: Path, obj: Any, mtime: float | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, indent=2)
    path.write_text(data + "\n")
    if mtime:
        path.touch()
        os.utime(path, (mtime, mtime))


def _trakt_session(client_id: str, access_token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(_TRAKT_API_HEADERS)
    session.headers["trakt-api-key"] = client_id
    session.headers["Authorization"] = f"Bearer {access_token}"

    retry_strategy = Retry(
        total=5,
        backoff_factor=60,
        status_forcelist=[429],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://api.trakt.tv", adapter)

    return session


def _trakt_api_get(ctx: Context, path: str, params: dict[str, str] = {}) -> Any:
    if not path.startswith("/"):
        path = f"/{path}"

    logger.info("GET %s", f"https://api.trakt.tv{path}")
    response = ctx.session.get(f"https://api.trakt.tv{path}", params=params)
    response.raise_for_status()

    if "x-pagination-page" in response.headers:
        raise ValueError("Paginated response not supported")

    return response.json()


def _trakt_api_paginated_get(
    ctx: Context,
    path: str,
    params: dict[str, str] = {},
) -> list[Any]:
    if not path.startswith("/"):
        path = f"/{path}"

    results: list[Any] = []

    page = 1
    limit = 1000
    page_count = 1
    item_count = 0

    while page <= page_count:
        params["page"] = str(page)
        params["limit"] = str(limit)

        logger.info("GET %s", f"https://api.trakt.tv{path}")
        response = ctx.session.get(f"https://api.trakt.tv{path}", params=params)
        response.raise_for_status()

        assert "x-pagination-page" in response.headers
        assert response.headers["x-pagination-page"] == str(page)
        assert "x-pagination-limit" in response.headers
        assert response.headers["x-pagination-limit"] == str(limit)
        assert "x-pagination-page-count" in response.headers
        page_count = int(response.headers["x-pagination-page-count"])
        assert "x-pagination-item-count" in response.headers
        item_count = int(response.headers["x-pagination-item-count"])

        results.extend(response.json())
        page += 1

    if len(results) != item_count:
        logger.warning(f"{path} has {len(results)} items, expected {item_count}")
    return results


def _export_user_last_activities(ctx: ExportContext) -> ExportLastActivities:
    output_path = ctx.output_dir / "user" / "last-activities.json"
    data = _trakt_api_get(ctx, path="/sync/last_activities")
    _write_json(output_path, data)
    return cast(ExportLastActivities, data)


def _compare_datetime_strs(a: str, b: str) -> bool:
    return datetime.fromisoformat(a) >= datetime.fromisoformat(b)


def _last_hidden_at_activities(activities: ExportLastActivities) -> datetime:
    return max(
        datetime.fromisoformat(activities["movies"]["hidden_at"]),
        datetime.fromisoformat(activities["shows"]["hidden_at"]),
        datetime.fromisoformat(activities["seasons"]["hidden_at"]),
    )


def _last_watched_at_activities(activities: ExportLastActivities) -> datetime:
    return max(
        datetime.fromisoformat(activities["movies"]["watched_at"]),
        datetime.fromisoformat(activities["episodes"]["watched_at"]),
    )


def _last_paused_at_activities(activities: ExportLastActivities) -> datetime:
    return max(
        datetime.fromisoformat(activities["movies"]["paused_at"]),
        datetime.fromisoformat(activities["episodes"]["paused_at"]),
    )


def _activities_outdated_paths(
    data_path: Path,
    old_activities: ExportLastActivities | None,
    new_activities: ExportLastActivities,
) -> tuple[list[Path], list[Path]]:
    fresh_paths: list[Path] = []
    stale_paths: list[Path] = []

    def _mark_path(path: Path, fresh: bool) -> None:
        if fresh:
            fresh_paths.append(path)
        else:
            stale_paths.append(path)

    exports = [
        ("movies", "collected_at", data_path / "collection" / "collection-movies.json"),
        ("movies", "watched_at", data_path / "watched" / "watched-movies.json"),
        ("movies", "rated_at", data_path / "ratings" / "ratings-movies.json"),
        ("movies", "commented_at", data_path / "comments" / "comments-movies.json"),
        (
            "episodes",
            "collected_at",
            data_path / "collection" / "collection-shows.json",
        ),
        ("episodes", "watched_at", data_path / "watched" / "watched-shows.json"),
        ("episodes", "watched_at", data_path / "watched" / "progress-shows.json"),
        ("episodes", "rated_at", data_path / "ratings" / "ratings-episodes.json"),
        ("episodes", "commented_at", data_path / "comments" / "comments-episodes.json"),
        ("shows", "rated_at", data_path / "ratings" / "ratings-shows.json"),
        ("shows", "commented_at", data_path / "comments" / "comments-shows.json"),
        ("seasons", "rated_at", data_path / "ratings" / "ratings-seasons.json"),
        ("seasons", "commented_at", data_path / "comments" / "comments-seasons.json"),
        ("comments", "liked_at", data_path / "likes" / "likes-comments.json"),
        ("lists", "liked_at", data_path / "likes" / "likes-lists.json"),
        ("lists", "updated_at", data_path / "lists" / "lists.json"),
        ("lists", "commented_at", data_path / "comments" / "comments-lists.json"),
        ("watchlist", "updated_at", data_path / "lists" / "watchlist.json"),
        ("account", "settings_at", data_path / "user" / "profile.json"),
    ]

    activities_fresh = False
    if old_activities:
        activities_fresh = _compare_datetime_strs(
            old_activities["all"], new_activities["all"]
        )
    _mark_path(data_path / "user" / "last-activities.json", activities_fresh)
    _mark_path(data_path / "user" / "stats.json", activities_fresh)

    history_fresh = False
    if old_activities:
        history_fresh = _last_watched_at_activities(
            new_activities
        ) >= _last_watched_at_activities(old_activities)
    _mark_path(data_path / "watched" / "history.json", history_fresh)

    playback_fresh = False
    if old_activities:
        playback_fresh = _last_paused_at_activities(
            new_activities
        ) >= _last_paused_at_activities(old_activities)
    _mark_path(data_path / "watched" / "playback.json", playback_fresh)

    for namespace_key, activity_key, path in exports:
        fresh = False
        if old_activities:
            old_date_str = cast(Any, old_activities)[namespace_key][activity_key]
            new_date_str = cast(Any, new_activities)[namespace_key][activity_key]
            fresh = _compare_datetime_strs(old_date_str, new_date_str)
        _mark_path(path, fresh)

    old_activities_hidden_at = datetime.fromtimestamp(0, tz=timezone.utc)
    if old_activities:
        old_activities_hidden_at = _last_hidden_at_activities(old_activities)
    new_activities_hidden_at = _last_hidden_at_activities(new_activities)
    hidden_at_fresh = old_activities_hidden_at >= new_activities_hidden_at
    _mark_path(data_path / "hidden" / "hidden-calendar.json", hidden_at_fresh)
    _mark_path(data_path / "hidden" / "hidden-dropped.json", hidden_at_fresh)
    _mark_path(data_path / "hidden" / "hidden-progress-collected.json", hidden_at_fresh)
    _mark_path(
        data_path / "hidden" / "hidden-progress-watched-reset.json", hidden_at_fresh
    )
    _mark_path(data_path / "hidden" / "hidden-progress-watched.json", hidden_at_fresh)
    _mark_path(data_path / "hidden" / "hidden-recommendations.json", hidden_at_fresh)

    return (fresh_paths, stale_paths)


def _excluded(ctx: ExportContext, path: Path) -> bool:
    for excluded_path in ctx.exclude_paths:
        if path == excluded_path:
            return True
        elif path.is_relative_to(excluded_path):
            return True
    return False


def _fresh(ctx: ExportContext, path: Path) -> bool:
    if not path.exists():
        return False
    elif path in ctx.fresh_paths:
        return True
    elif path in ctx.stale_paths:
        return False
    else:
        logger.warning("Path freshness is unknown: %s", path)
        return False


def _export_user_profile(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "user" / "profile.json"

    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
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


def _export_user_stats(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "user" / "stats.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_get(ctx, path="/users/me/stats")
    _write_json(output_path, data)


def _read_json_data(path: Path, return_type: type[T]) -> T:
    return cast(T, json.loads(path.read_text()))


def _export_watched_history(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "watched" / "history.json"

    if _fresh(ctx, output_path):
        return

    if output_path.exists():
        existing_items = _read_json_data(output_path, list[HistoryItem])
        start_at = existing_items[0]["watched_at"]

        new_items = _trakt_api_paginated_get(
            ctx,
            path="/sync/history",
            params={"start_at": start_at},
        )
        if len(new_items) <= 1:
            logger.info("No new items watched since %s", start_at)
            return

    data = _trakt_api_paginated_get(ctx, path="/sync/history")
    _write_json(output_path, data)


def _export_watched_playback(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "watched" / "playback.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_get(ctx, path="/sync/playback")
    _write_json(output_path, data)


def _export_watched(
    ctx: ExportContext,
    type: Literal["movies", "shows"],
) -> None:
    output_path = ctx.output_dir / "watched" / f"watched-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_get(ctx, path=f"/sync/watched/{type}")
    _write_json(output_path, data)


def _export_collection(
    ctx: ExportContext,
    type: Literal["movies", "shows"],
) -> None:
    output_path = ctx.output_dir / "collection" / f"collection-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_get(ctx, path=f"/sync/collection/{type}")
    _write_json(output_path, data)


def _export_comments(
    ctx: ExportContext,
    type: Literal["movies", "shows", "seasons", "episodes", "lists"],
) -> None:
    output_path = ctx.output_dir / "comments" / f"comments-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_paginated_get(ctx, path=f"/users/me/comments/{type}")
    _write_json(output_path, data)


def _export_hidden(ctx: ExportContext, section: str) -> None:
    output_path = ctx.output_dir / "hidden" / f"hidden-{section.replace('_', '-')}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_paginated_get(ctx, path=f"/users/hidden/{section}")
    _write_json(output_path, data)


def _export_likes(
    ctx: ExportContext,
    type: Literal["comments", "lists"],
) -> None:
    output_path = ctx.output_dir / "likes" / f"likes-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_paginated_get(ctx, path=f"/users/me/likes/{type}")
    _write_json(output_path, data)


def _export_lists_list(ctx: ExportContext, list_id: int, list_slug: str) -> None:
    output_path = ctx.output_dir / "lists" / f"list-{list_id}-{list_slug}.json"

    if _excluded(ctx, output_path):
        return
    elif output_path.exists() and output_path in ctx.fresh_paths:
        return

    data = _trakt_api_paginated_get(ctx, path=f"/users/me/lists/{list_id}/items")
    _write_json(output_path, data)


def _export_lists_list_all(ctx: ExportContext, lists: list[List]) -> None:
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


def _export_lists_lists(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "lists" / "lists.json"

    if _excluded(ctx, output_path):
        return

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(ctx, path="/users/me/lists")
    _write_json(output_path, data)
    _export_lists_list_all(ctx, cast(list[List], data))


def _export_lists_watchlist(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "lists" / "watchlist.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_paginated_get(
        ctx,
        path="/sync/watchlist",
        params={"sort_by": "rank", "sort_how": "asc"},
    )
    _write_json(output_path, data)


def _export_ratings(
    ctx: ExportContext,
    type: Literal["movies", "shows", "seasons", "episodes"],
) -> None:
    output_path = ctx.output_dir / "ratings" / f"ratings-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = _trakt_api_get(ctx, path=f"/sync/ratings/{type}")
    _write_json(output_path, data)


def _export_shows_watched_progress(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "watched" / "progress-shows.json"

    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return

    watched_shows = _read_json_data(
        ctx.output_dir / "watched" / "watched-shows.json",
        list[WatchedShow],
    )

    shows: list[Any] = []
    for watched_show in watched_shows:
        show_id = watched_show["show"]["ids"]["trakt"]
        path = f"/shows/{show_id}/progress/watched"
        data = _trakt_api_get(ctx, path=path)
        show_progress = {
            "show": watched_show["show"],
            "progress": data,
        }
        shows.append(show_progress)

    _write_json(output_path, shows)


@click.group()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
def main(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)


@main.command()
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
    "--exclude",
    type=click.Path(file_okay=True, dir_okay=True),
    required=False,
    multiple=True,
    envvar="TRAKT_DATA_EXCLUDE",
    help="Exclude paths from export",
)
def export(
    trakt_client_id: str,
    trakt_access_token: str,
    output_dir: Path,
    exclude: list[str],
) -> None:
    _session = _trakt_session(
        client_id=trakt_client_id,
        access_token=trakt_access_token,
    )

    exclude_paths: list[Path] = []
    for path in exclude:
        if path.startswith(".") or path.startswith("/"):
            exclude_paths.append(Path(path))
        else:
            exclude_paths.append(output_dir / path)

    logger.debug("exclude_paths: %s", exclude_paths)

    ctx = ExportContext(
        session=_session,
        output_dir=output_dir,
        exclude_paths=exclude_paths,
        fresh_paths=[],
        stale_paths=[],
    )

    old_activities: ExportLastActivities | None = None
    if (output_dir / "user" / "last-activities.json").exists():
        old_activities = _read_json_data(
            output_dir / "user" / "last-activities.json",
            ExportLastActivities,
        )
    last_activities = _export_user_last_activities(ctx)

    fresh_paths, stale_paths = _activities_outdated_paths(
        data_path=output_dir,
        old_activities=old_activities,
        new_activities=last_activities,
    )
    ctx.fresh_paths = fresh_paths
    ctx.stale_paths = stale_paths

    logger.debug("fresh_paths: %s", fresh_paths)
    if stale_paths:
        logger.info("stale_paths: %s", stale_paths)

    _export_collection(ctx, type="movies")
    _export_collection(ctx, type="shows")
    _export_comments(ctx, type="episodes")
    _export_comments(ctx, type="lists")
    _export_comments(ctx, type="movies")
    _export_comments(ctx, type="seasons")
    _export_comments(ctx, type="shows")
    _export_hidden(ctx, section="calendar")
    _export_hidden(ctx, section="dropped")
    _export_hidden(ctx, section="progress_collected")
    _export_hidden(ctx, section="progress_watched_reset")
    _export_hidden(ctx, section="progress_watched")
    _export_hidden(ctx, section="recommendations")
    _export_likes(ctx, type="comments")
    _export_likes(ctx, type="lists")
    _export_lists_lists(ctx)
    _export_lists_watchlist(ctx)
    _export_ratings(ctx, type="episodes")
    _export_ratings(ctx, type="movies")
    _export_ratings(ctx, type="seasons")
    _export_ratings(ctx, type="shows")
    _export_user_profile(ctx)
    _export_user_stats(ctx)
    _export_watched_history(ctx)
    _export_watched_playback(ctx)
    _export_watched(ctx, type="movies")
    _export_watched(ctx, type="shows")

    # Non-standard export
    _export_shows_watched_progress(ctx)


def partition_filename(basedir: Path, id: int, suffix: str) -> Path:
    id_str = str(id)
    if len(id_str) == 1:
        id_prefix = id_str + "0"
    else:
        id_prefix = id_str[:2]
    return basedir / id_prefix / f"{id}{suffix}"


def _export_media_movie(ctx: MetricsContext, trakt_id: int) -> MovieExtended:
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "movies",
        id=trakt_id,
        suffix=".json",
    )

    if output_path.exists():
        return _read_json_data(output_path, MovieExtended)

    data = _trakt_api_get(ctx, path=f"/movies/{trakt_id}", params={"extended": "full"})
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    _write_json(output_path, data, mtime=mtime)
    return cast(MovieExtended, data)


def _export_media_show(ctx: MetricsContext, trakt_id: int) -> ShowExtended:
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "shows",
        id=trakt_id,
        suffix=".json",
    )

    if output_path.exists():
        return _read_json_data(output_path, ShowExtended)

    data = _trakt_api_get(ctx, path=f"/shows/{trakt_id}", params={"extended": "full"})
    seasons_data = _trakt_api_get(ctx, path=f"/shows/{trakt_id}/seasons")
    data["seasons"] = seasons_data
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    _write_json(output_path, data, mtime=mtime)
    return cast(ShowExtended, data)


def _load_cached_media_season(
    ctx: MetricsContext,
    trakt_id: int,
) -> SeasonExtended | None:
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "seasons",
        id=trakt_id,
        suffix=".json",
    )
    if output_path.exists():
        return _read_json_data(output_path, SeasonExtended)
    return None


def _fetch_media_season(
    ctx: MetricsContext,
    show_trakt_id: int,
    season_number: int,
) -> SeasonExtended:
    data = _trakt_api_get(
        ctx,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}/info",
        params={"extended": "full"},
    )
    episodes_data = _trakt_api_get(
        ctx,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}",
    )
    data["episodes"] = episodes_data
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "seasons",
        id=data["ids"]["trakt"],
        suffix=".json",
    )
    _write_json(output_path, data, mtime=mtime)
    return cast(SeasonExtended, data)


def _export_media_season(
    ctx: MetricsContext,
    show_trakt_id: int,
    season_trakt_id: int,
    season_number: int,
) -> SeasonExtended:
    if season := _load_cached_media_season(ctx, season_trakt_id):
        return season
    return _fetch_media_season(
        ctx,
        show_trakt_id=show_trakt_id,
        season_number=season_number,
    )


def _export_media_season2(
    ctx: MetricsContext,
    show: ShowExtended,
    season_number: int,
) -> SeasonExtended:
    season_trakt_id: int | None = None
    for season in show["seasons"]:
        if season["number"] == season_number:
            season_trakt_id = season["ids"]["trakt"]
            break

    if season_trakt_id:
        if extended_season := _load_cached_media_season(ctx, season_trakt_id):
            return extended_season
    else:
        logger.warning(
            "Season #%d not found in show '%s'",
            season_number,
            show["title"],
        )

    return _fetch_media_season(
        ctx,
        show_trakt_id=show["ids"]["trakt"],
        season_number=season_number,
    )


def _load_cached_media_episode(
    ctx: MetricsContext,
    trakt_id: int,
) -> EpisodeExtended | None:
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "episodes",
        id=trakt_id,
        suffix=".json",
    )
    if output_path.exists():
        return _read_json_data(output_path, EpisodeExtended)
    return None


def _fetch_media_episode(
    ctx: MetricsContext,
    show_trakt_id: int,
    season_number: int,
    episode_number: int,
) -> EpisodeExtended:
    data = _trakt_api_get(
        ctx,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}/episodes/{episode_number}",
        params={"extended": "full"},
    )
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "episodes",
        id=data["ids"]["trakt"],
        suffix=".json",
    )
    _write_json(output_path, data, mtime=mtime)
    return cast(EpisodeExtended, data)


def _export_media_episode(
    ctx: MetricsContext,
    trakt_id: int,
    show_trakt_id: int,
    season: int,
    number: int,
) -> EpisodeExtended:
    if episode := _load_cached_media_episode(ctx, trakt_id):
        return episode

    return _fetch_media_episode(
        ctx,
        show_trakt_id=show_trakt_id,
        season_number=season,
        episode_number=number,
    )


def _export_media_episode2(
    ctx: MetricsContext,
    show_trakt_id: int,
    season: SeasonExtended,
    episode_number: int,
) -> EpisodeExtended:
    episode_trakt_id: int | None = None
    for episode in season["episodes"]:
        if episode["number"] == episode_number:
            episode_trakt_id = episode["ids"]["trakt"]
            break

    if episode_trakt_id:
        if extended_episode := _load_cached_media_episode(ctx, episode_trakt_id):
            return extended_episode
    else:
        logger.warning(
            "Episode #%d not found in season #%d of show #%d",
            episode_number,
            season["number"],
            show_trakt_id,
        )

    return _fetch_media_episode(
        ctx,
        show_trakt_id=show_trakt_id,
        season_number=season["number"],
        episode_number=episode_number,
    )


def _episode_first_aired_year(
    episode: EpisodeExtended, show: ShowExtended
) -> int | None:
    year = show.get("year")
    if episode.get("first_aired"):
        year = int(episode["first_aired"].split("-")[0])
    return year


def _generate_collection_metrics(ctx: MetricsContext, data_path: Path) -> None:
    movies_collection = _read_json_data(
        data_path / "collection" / "collection-movies.json", list[CollectedMovie]
    )
    for collected_movie in movies_collection:
        trakt_id = collected_movie["movie"]["ids"]["trakt"]
        movie = _export_media_movie(ctx, trakt_id=trakt_id)
        year_str = str(movie["year"] or _FUTURE_YEAR)
        _TRAKT_COLLECTION_COUNT.labels(
            media_type="movie",
            year=year_str,
        ).inc()

    shows_collection = _read_json_data(
        data_path / "collection" / "collection-shows.json", list[CollectedShow]
    )
    for collected_show in shows_collection:
        show_trakt_id = collected_show["show"]["ids"]["trakt"]
        show = _export_media_show(ctx, trakt_id=show_trakt_id)
        show_year_str = str(show["year"] or _FUTURE_YEAR)
        _TRAKT_COLLECTION_COUNT.labels(
            media_type="show",
            year=show_year_str,
        ).inc()

        for collected_season in collected_show["seasons"]:
            season = _export_media_season2(
                ctx,
                show=show,
                season_number=collected_season["number"],
            )
            for collected_episode in collected_season["episodes"]:
                episode = _export_media_episode2(
                    ctx,
                    show_trakt_id=show["ids"]["trakt"],
                    season=season,
                    episode_number=collected_episode["number"],
                )
                episode_year_str = str(
                    _episode_first_aired_year(episode, show) or _FUTURE_YEAR
                )
                _TRAKT_COLLECTION_COUNT.labels(
                    media_type="episode",
                    year=episode_year_str,
                ).inc()


def _generate_ratings_metrics(ctx: MetricsContext, data_path: Path) -> None:
    episode_ratings = _read_json_data(
        data_path / "ratings" / "ratings-episodes.json", list[EpisodeRating]
    )
    for episode_rating in episode_ratings:
        episode = _export_media_episode(
            ctx,
            trakt_id=episode_rating["episode"]["ids"]["trakt"],
            show_trakt_id=episode_rating["show"]["ids"]["trakt"],
            season=episode_rating["episode"]["season"],
            number=episode_rating["episode"]["number"],
        )
        show = _export_media_show(
            ctx,
            trakt_id=episode_rating["show"]["ids"]["trakt"],
        )
        year_str = str(_episode_first_aired_year(episode, show) or _FUTURE_YEAR)
        rating_str = str(episode_rating["rating"])
        _TRAKT_RATINGS_COUNT.labels(
            media_type="episode",
            year=year_str,
            rating=rating_str,
        ).inc()

    movie_ratings = _read_json_data(
        data_path / "ratings" / "ratings-movies.json", list[MovieRating]
    )
    for movie_rating in movie_ratings:
        movie = _export_media_movie(ctx, trakt_id=movie_rating["movie"]["ids"]["trakt"])
        year_str = str(movie["year"] or _FUTURE_YEAR)
        rating_str = str(movie_rating["rating"])
        _TRAKT_RATINGS_COUNT.labels(
            media_type="movie",
            year=year_str,
            rating=rating_str,
        ).inc()

    # TODO: seasons

    show_ratings = _read_json_data(
        data_path / "ratings" / "ratings-shows.json", list[ShowRating]
    )
    for show_rating in show_ratings:
        show = _export_media_show(ctx, trakt_id=show_rating["show"]["ids"]["trakt"])
        year_str = str(show["year"] or _FUTURE_YEAR)
        rating_str = str(show_rating["rating"])
        _TRAKT_RATINGS_COUNT.labels(
            media_type="show",
            year=year_str,
            rating=rating_str,
        ).inc()


def _generate_watched_metrics(ctx: MetricsContext, data_path: Path) -> None:
    history_items = _read_json_data(
        data_path / "watched" / "history.json", list[HistoryItem]
    )
    for history_item in history_items:
        if history_item["type"] == "movie":
            movie = _export_media_movie(
                ctx, trakt_id=history_item["movie"]["ids"]["trakt"]
            )
            year_str = str(movie["year"] or _FUTURE_YEAR)
            runtime = movie["runtime"]
            _TRAKT_WATCHED_COUNT.labels(
                media_type="movie",
                year=year_str,
            ).inc()
            _TRAKT_WATCHED_RUNTIME.labels(
                media_type="movie",
                year=year_str,
            ).inc(runtime)
        elif history_item["type"] == "episode":
            # TODO
            pass


def _generate_watchlist_metrics(ctx: MetricsContext, data_path: Path) -> None:
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


@main.command()
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
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=_default_cache_dir(),
    show_default=True,
)
def metrics(
    trakt_client_id: str,
    trakt_access_token: str,
    output_dir: Path,
    cache_dir: Path,
) -> None:
    _session = _trakt_session(
        client_id=trakt_client_id,
        access_token=trakt_access_token,
    )
    ctx = MetricsContext(session=_session, output_dir=output_dir, cache_dir=cache_dir)

    user_profile = _read_json_data(
        output_dir / "user" / "profile.json",
        ExportUserProfile,
    )
    username = user_profile["username"]

    _TRAKT_VIP_YEARS.labels(username=username).set(user_profile["vip_years"])

    _generate_collection_metrics(ctx, output_dir)
    _generate_ratings_metrics(ctx, output_dir)
    _generate_watched_metrics(ctx, output_dir)
    _generate_watchlist_metrics(ctx, output_dir)

    metrics_path: str = str(output_dir / "metrics.prom")
    write_to_textfile(metrics_path, _REGISTRY)


def _parse_timedelta(value: str) -> timedelta:
    if value == "" or value == "0":
        return timedelta(days=0)
    elif value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Invalid time delta: {value}")


def _parse_abs_limit(limit: str, total: int) -> int:
    if limit.endswith("%"):
        return int(total * float(limit[:-1]) / 100)
    return int(limit)


def _weighted_shuffle(values: list[float], limit: int) -> list[int]:
    weights = [1.0 / v for v in values]
    total = sum(weights)
    assert total > 0

    normalized_weights = [w / total for w in weights]
    randomized_weights = [
        (w * random.random(), i) for i, w in enumerate(normalized_weights)
    ]

    randomized_weights.sort(reverse=True)
    indices = [i for _, i in randomized_weights]
    return indices if len(indices) <= limit else indices[:limit]


@main.command()
@click.option(
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=_default_cache_dir(),
    show_default=True,
)
@click.option(
    "--min-age",
    type=str,
    envvar="TRAKT_DATA_CACHE_MIN_AGE",
    default="1d",
    show_default=True,
)
@click.option(
    "--limit",
    type=str,
    envvar="TRAKT_DATA_CACHE_LIMIT",
    default="1%",
    show_default=True,
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    show_default=True,
)
def prune_cache(
    cache_dir: Path,
    min_age: str,
    limit: str,
    dry_run: bool,
) -> None:
    now = datetime.now()
    min_mtime: datetime = now - _parse_timedelta(min_age)
    files: list[tuple[Path, datetime, float]] = []

    for file in cache_dir.glob("**/*.json"):
        mtime = datetime.fromtimestamp(file.stat().st_mtime)
        age = (now - mtime).total_seconds()
        assert age > 0
        if mtime < min_mtime:
            files.append((file, mtime, age))

    files.sort(key=lambda f: f[2])

    if len(files) == 0:
        logger.info("Cache is empty")
        return

    limit_abs = _parse_abs_limit(limit, len(files))
    expired_indices = _weighted_shuffle([f[2] for f in files], limit=limit_abs)
    assert len(expired_indices) <= limit_abs

    if len(expired_indices) == 0:
        logger.info("No cache files to prune")
        return

    logger.info(
        "Pruning %.2f%% of cache, %d/%d files",
        len(expired_indices) / len(files) * 100,
        len(expired_indices),
        len(files),
    )

    expired_indices.sort()
    for idx in expired_indices:
        file, mtime, age = files[idx]
        age_dt = timedelta(seconds=int(age))
        logger.info("Prune '%s' (%s, %s)", file, mtime, age_dt)
        if not dry_run:
            file.unlink()


@main.command()
@click.option(
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=_default_cache_dir(),
    show_default=True,
)
def cache_stats(
    cache_dir: Path,
) -> None:
    ages = [_NOW - file.stat().st_mtime for file in cache_dir.glob("**/*.json")]
    ages.sort()

    mean_age = sum(ages) / len(ages)
    median_age = ages[len(ages) // 2]
    p75_age = ages[int(len(ages) * 0.75)]
    p95_age = ages[int(len(ages) * 0.95)]
    p99_age = ages[int(len(ages) * 0.99)]
    min_age = ages[0]
    max_age = ages[-1]

    click.echo(f"mean age: {timedelta(seconds=int(mean_age))}")
    click.echo(f"median age: {timedelta(seconds=int(median_age))}")
    click.echo(f"75th percentile age: {timedelta(seconds=int(p75_age))}")
    click.echo(f"95th percentile age: {timedelta(seconds=int(p95_age))}")
    click.echo(f"99th percentile age: {timedelta(seconds=int(p99_age))}")
    click.echo(f"min age: {timedelta(seconds=int(min_age))}")
    click.echo(f"max age: {timedelta(seconds=int(max_age))}")
    click.echo(f"total files: {len(ages)}")


@main.command()
@click.option(
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=_default_cache_dir(),
    show_default=True,
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    show_default=True,
)
def fix_mtimes(
    cache_dir: Path,
    dry_run: bool,
) -> None:
    for file in cache_dir.glob("**/*.json"):
        data = json.loads(file.read_text())
        if not isinstance(data, dict):
            logger.warning("File '%s' is not a dictionary", file)
            continue
        if "updated_at" not in data:
            logger.warning("File '%s' is missing 'updated_at' key", file)
            continue
        actual_mtime = file.stat().st_mtime
        expected_mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
        if actual_mtime == expected_mtime:
            continue
        if not dry_run:
            logger.warning(
                "Fixing '%s' (actual: %s, expected: %s)",
                file,
                datetime.fromtimestamp(actual_mtime),
                datetime.fromtimestamp(expected_mtime),
            )
            os.utime(file, (expected_mtime, expected_mtime))


if __name__ == "__main__":
    main()
