import json
import logging
import os
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


class EpisodeExtended(TypedDict):
    season: int
    number: int
    title: str
    ids: "EpisodeIDs"
    first_aired: str
    updated_at: str
    runtime: int
    episode_type: str


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


class Episode(TypedDict):
    season: int
    number: int
    title: str
    ids: "EpisodeIDs"


class EpisodeIDs(TypedDict):
    trakt: int
    tvdb: int
    imdb: str | None
    tmdb: int
    tvrage: int | None


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


class Context:
    session: requests.Session
    output_dir: Path
    cache_dir: Path
    expired_data_files: set[Path]

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
        cache_dir: Path,
        expired_data_files: set[Path] = set(),
    ) -> None:
        self.session = session
        self.output_dir = output_dir
        self.cache_dir = cache_dir
        self.expired_data_files = expired_data_files

        cache_dir.mkdir(parents=True, exist_ok=True)


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


def _default_cache_dir() -> Path:
    return _xdg_cache_home() / "trakt-data"


def _file_updated_at(data_path: Path, cache_path: Path, filename: Path) -> datetime:
    mtime = datetime.fromtimestamp(filename.stat().st_mtime, tz=timezone.utc)
    updated_at = mtime
    if filename.is_relative_to(data_path):
        base_path = data_path
    elif filename.is_relative_to(cache_path):
        base_path = cache_path
    else:
        raise ValueError(
            f"File {filename} is not relative to {data_path} or {cache_path}"
        )
    relative_path = str(filename.relative_to(base_path))
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

    assert updated_at, "updated_at is not set"
    assert updated_at.tzinfo, "updated_at is not offset-aware"
    return updated_at


def _weighted_shuffle(
    data_path: Path,
    cache_path: Path,
    files: list[Path],
) -> list[Path]:
    now = datetime.now(tz=timezone.utc)

    ages: dict[Path, timedelta] = {
        file: now - _file_updated_at(data_path, cache_path, file) for file in files
    }
    file_weights: dict[Path, float] = {
        file: 1.0 / (1.0 + (ages[file] / timedelta(days=1))) for file in files
    }

    def random_key(file: Path) -> float:
        return float(random.random() ** (1.0 / max(file_weights[file], 0.0001)))

    return sorted(files, key=random_key, reverse=True)


class ExportContext(Context):
    def _compute_expired_data_files(
        self,
        data_path: Path,
        cache_path: Path,
        limit: int = 10,
    ) -> set[Path]:
        files = []

        for file in data_path.glob("**/*.json"):
            if file.is_relative_to(data_path / "media"):
                continue
            files.append(file)

        expired_files = _weighted_shuffle(data_path, cache_path, files)[:limit]

        if len(files) > 0:
            logger.info(
                "Expired files: %d/%d (%.2f%%)",
                len(expired_files),
                len(files),
                len(expired_files) / len(files) * 100,
            )

        for file in expired_files:
            logger.debug(
                "Expiring '%s' (modified %s)",
                file,
                _file_updated_at(data_path, cache_path, file),
            )

        return set(expired_files)

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
        cache_dir: Path,
    ) -> None:
        expired_data_files = self._compute_expired_data_files(
            data_path=output_dir,
            cache_path=cache_dir,
        )
        super().__init__(session, output_dir, cache_dir, expired_data_files)


class MetricsContext(Context):
    def _compute_expired_media_files(
        self,
        data_path: Path,
        cache_path: Path,
        limit: int = 250,
        min_media_age: timedelta = timedelta(days=1),
    ) -> set[Path]:
        media_path = data_path / "media"
        min_media_mtime = datetime.now(tz=timezone.utc) - min_media_age

        files = []

        for file in media_path.glob("**/*.json"):
            if _file_updated_at(data_path, cache_path, file) < min_media_mtime:
                files.append(file)

        for file in cache_path.joinpath("media").glob("**/*.json"):
            if _file_updated_at(data_path, cache_path, file) < min_media_mtime:
                files.append(file)

        expired_files = _weighted_shuffle(data_path, cache_path, files)[:limit]

        if len(files) > 0:
            logger.info(
                "Expired media files: %d/%d (%.2f%%)",
                len(expired_files),
                len(files),
                len(expired_files) / len(files) * 100,
            )

        for file in expired_files:
            logger.debug(
                "Expiring '%s' (modified %s)",
                file,
                _file_updated_at(data_path, cache_path, file),
            )

        return set(expired_files)

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
        cache_dir: Path,
    ) -> None:
        expired_data_files = self._compute_expired_media_files(
            data_path=output_dir,
            cache_path=cache_dir,
        )
        super().__init__(session, output_dir, cache_dir, expired_data_files)


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


def _read_json_data(path: Path, return_type: type[T]) -> T:
    return cast(T, json.loads(path.read_text()))


def _export_watched_history(ctx: Context) -> None:
    output_path = ctx.output_dir / "watched" / "history.json"

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


def _export_watched_playback(ctx: Context) -> None:
    output_path = ctx.output_dir / "watched" / "playback.json"

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(ctx, path="/sync/playback")
    _write_json(output_path, data)


def _export_watched(
    ctx: Context,
    type: Literal["movies", "shows"],
    filename: str,
) -> None:
    output_path = ctx.output_dir / "watched" / filename

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(ctx, path=f"/sync/watched/{type}")
    _write_json(output_path, data)


def _export_watched_movies(ctx: Context) -> None:
    _export_watched(ctx, type="movies", filename="watched-movies.json")


def _export_watched_shows(ctx: Context) -> None:
    _export_watched(ctx, type="shows", filename="watched-shows.json")


def _export_collection(
    ctx: Context,
    type: Literal["movies", "shows"],
    filename: str,
) -> None:
    output_path = ctx.output_dir / "collection" / filename

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(ctx, path=f"/users/me/collection/{type}")
    _write_json(output_path, data)


def _export_collection_movies(ctx: ExportContext) -> None:
    _export_collection(ctx, type="movies", filename="collection-movies.json")


def _export_collection_shows(ctx: ExportContext) -> None:
    _export_collection(ctx, type="shows", filename="collection-shows.json")


def _export_comments(
    ctx: Context,
    type: Literal["all", "movies", "shows", "seasons", "episodes", "lists"],
    filename: str,
) -> None:
    output_path = ctx.output_dir / "comments" / filename

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_paginated_get(ctx, path=f"/users/me/comments/{type}")
    _write_json(output_path, data)


def _export_comments_episodes(ctx: ExportContext) -> None:
    _export_comments(ctx, type="episodes", filename="comments-episodes.json")


def _export_comments_lists(ctx: ExportContext) -> None:
    _export_comments(ctx, type="lists", filename="comments-lists.json")


def _export_comments_movies(ctx: ExportContext) -> None:
    _export_comments(ctx, type="movies", filename="comments-movies.json")


def _export_comments_seasons(ctx: ExportContext) -> None:
    _export_comments(ctx, type="seasons", filename="comments-seasons.json")


def _export_comments_shows(ctx: ExportContext) -> None:
    _export_comments(ctx, type="shows", filename="comments-shows.json")


def _export_hidden(
    ctx: Context,
    section: str,
    filename: str,
) -> None:
    output_path = ctx.output_dir / "hidden" / filename

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_paginated_get(ctx, path=f"/users/hidden/{section}")
    _write_json(output_path, data)


def _export_hidden_calendar(ctx: ExportContext) -> None:
    _export_hidden(
        ctx,
        section="calendar",
        filename="hidden-calendar.json",
    )


def _export_hidden_dropped(ctx: ExportContext) -> None:
    _export_hidden(
        ctx,
        section="dropped",
        filename="hidden-dropped.json",
    )


def _export_hidden_progress_collected(ctx: ExportContext) -> None:
    _export_hidden(
        ctx,
        section="progress_collected",
        filename="hidden-progress-collected.json",
    )


def _export_hidden_progress_watched_reset(ctx: ExportContext) -> None:
    _export_hidden(
        ctx,
        section="progress_watched_reset",
        filename="hidden-progress-watched-reset.json",
    )


def _export_hidden_progress_watched(ctx: ExportContext) -> None:
    _export_hidden(
        ctx,
        section="progress_watched",
        filename="hidden-progress-watched.json",
    )


def _export_hidden_recommendations(ctx: ExportContext) -> None:
    _export_hidden(
        ctx,
        section="recommendations",
        filename="hidden-recommendations.json",
    )


def _export_likes(
    ctx: Context,
    type: Literal["comments", "lists"],
    filename: str,
) -> None:
    output_path = ctx.output_dir / "likes" / filename

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_paginated_get(ctx, path=f"/users/me/likes/{type}")
    _write_json(output_path, data)


def _export_likes_comments(ctx: ExportContext) -> None:
    _export_likes(ctx, type="comments", filename="likes-comments.json")


def _export_likes_lists(ctx: ExportContext) -> None:
    _export_likes(ctx, type="lists", filename="likes-lists.json")


def _export_lists_list(ctx: ExportContext, list_id: int, list_slug: str) -> None:
    output_path = ctx.output_dir / "lists" / f"list-{list_id}-{list_slug}.json"

    if _fresh(ctx, output_path):
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

    if not _fresh(ctx, output_path):
        data = _trakt_api_get(ctx, path="/users/me/lists")
        _write_json(output_path, data)

    assert output_path.exists()
    lists = _read_json_data(output_path, list[List])
    _export_lists_list_all(ctx, lists)


def _export_lists_watchlist(ctx: ExportContext) -> None:
    output_path = ctx.output_dir / "lists" / "watchlist.json"

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_paginated_get(
        ctx,
        path="/users/me/watchlist",
        params={"sort_by": "rank", "sort_how": "asc"},
    )
    _write_json(output_path, data)


def _export_ratings(
    ctx: ExportContext,
    type: Literal["movies", "shows", "seasons", "episodes", "all"],
    filename: str,
) -> None:
    output_path = ctx.output_dir / "ratings" / filename

    if _fresh(ctx, output_path):
        return

    data = _trakt_api_get(ctx, path=f"/users/me/ratings/{type}")
    _write_json(output_path, data)


def _export_ratings_episodes(ctx: ExportContext) -> None:
    _export_ratings(ctx, type="episodes", filename="ratings-episodes.json")


def _export_ratings_movies(ctx: ExportContext) -> None:
    _export_ratings(ctx, type="movies", filename="ratings-movies.json")


def _export_ratings_seasons(ctx: ExportContext) -> None:
    _export_ratings(ctx, type="seasons", filename="ratings-seasons.json")


def _export_ratings_shows(ctx: ExportContext) -> None:
    _export_ratings(ctx, type="shows", filename="ratings-shows.json")


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
    mtime = datetime.fromisoformat(movie["updated_at"]).timestamp()
    _write_json(output_path, movie, mtime=mtime)
    return movie


def _export_media_show(ctx: MetricsContext, trakt_id: int) -> ShowExtended:
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "shows",
        id=trakt_id,
        suffix=".json",
    )

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
    mtime = datetime.fromisoformat(show["updated_at"]).timestamp()
    _write_json(output_path, show, mtime=mtime)
    return show


def _export_media_episode(
    ctx: MetricsContext,
    trakt_id: int,
    show_trakt_id: int,
    season: int,
    number: int,
) -> EpisodeExtended:
    output_path = partition_filename(
        basedir=ctx.cache_dir / "media" / "episodes",
        id=trakt_id,
        suffix=".json",
    )

    if _fresh(ctx, output_path):
        return _read_json_data(output_path, EpisodeExtended)

    data = _trakt_api_get(
        ctx,
        path=f"/shows/{show_trakt_id}/seasons/{season}/episodes/{number}",
        params={"extended": "full"},
    )
    episode: EpisodeExtended = {
        "season": data["season"],
        "number": data["number"],
        "title": data["title"],
        "ids": data["ids"],
        "first_aired": data["first_aired"],
        "updated_at": data["updated_at"],
        "runtime": data["runtime"],
        "episode_type": data["episode_type"],
    }
    mtime = datetime.fromisoformat(episode["updated_at"]).timestamp()
    _write_json(output_path, episode, mtime=mtime)
    return episode


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
        trakt_id = collected_show["show"]["ids"]["trakt"]
        show = _export_media_show(ctx, trakt_id=trakt_id)
        year_str = str(show["year"] or _FUTURE_YEAR)
        _TRAKT_COLLECTION_COUNT.labels(
            media_type="show",
            year=year_str,
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
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=_default_cache_dir(),
    show_default=True,
)
def export(
    trakt_client_id: str,
    trakt_access_token: str,
    output_dir: Path,
    cache_dir: Path,
) -> None:
    _session = _trakt_session(
        client_id=trakt_client_id,
        access_token=trakt_access_token,
    )
    ctx = ExportContext(
        session=_session,
        output_dir=output_dir,
        cache_dir=cache_dir,
    )

    _export_collection_movies(ctx)
    _export_collection_shows(ctx)
    _export_comments_episodes(ctx)
    _export_comments_lists(ctx)
    _export_comments_movies(ctx)
    _export_comments_seasons(ctx)
    _export_comments_shows(ctx)
    _export_hidden_calendar(ctx)
    _export_hidden_dropped(ctx)
    _export_hidden_progress_collected(ctx)
    _export_hidden_progress_watched_reset(ctx)
    _export_hidden_progress_watched(ctx)
    _export_hidden_recommendations(ctx)
    _export_likes_comments(ctx)
    _export_likes_lists(ctx)
    _export_lists_lists(ctx)
    _export_lists_watchlist(ctx)
    _export_ratings_episodes(ctx)
    _export_ratings_movies(ctx)
    _export_ratings_seasons(ctx)
    _export_ratings_shows(ctx)
    _export_user_profile(ctx)
    _export_user_stats(ctx)
    _export_watched_history(ctx)
    _export_watched_playback(ctx)
    _export_watched_movies(ctx)
    _export_watched_shows(ctx)


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
    # _generate_watched_metrics(ctx, output_dir)
    _generate_watchlist_metrics(ctx, output_dir)

    metrics_path: str = str(output_dir / "metrics.prom")
    write_to_textfile(metrics_path, _REGISTRY)


if __name__ == "__main__":
    main()
