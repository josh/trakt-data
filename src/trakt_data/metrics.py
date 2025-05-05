from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, cast

import requests
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

from . import logger
from .export import read_json_data, write_json
from .trakt import (
    MOVIE_RELEASE_TYPES,
    CollectedMovie,
    CollectedShow,
    EpisodeExtended,
    EpisodeRating,
    HistoryItem,
    List,
    ListItem,
    MovieExtended,
    MovieRating,
    MovieReleaseType,
    SeasonExtended,
    Show,
    ShowExtended,
    ShowRating,
    UserProfile,
    trakt_api_get,
)

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

_TRAKT_LIST_COUNT = Gauge(
    "trakt_list_count",
    documentation="Number of items in Trakt lists",
    labelnames=["list", "media_type", "year", "status"],
    registry=_REGISTRY,
)

_TRAKT_LIST_MINUTES = Gauge(
    "trakt_list_minutes",
    documentation="Number of minutes in Trakt lists",
    labelnames=["list", "media_type", "year", "status"],
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

_TRAKT_WATCHED_MINUTES = Gauge(
    "trakt_watched_minutes",
    documentation="Number of minutes in Trakt watched",
    labelnames=["media_type", "year"],
    registry=_REGISTRY,
)

_TRAKT_WATCHLIST_COUNT = Gauge(
    "trakt_watchlist_count",
    documentation="Number of items in Trakt watchlist",
    labelnames=["media_type", "year", "status"],
    registry=_REGISTRY,
)

_TRAKT_WATCHLIST_MINUTES = Gauge(
    "trakt_watchlist_minutes",
    documentation="Number of minutes in Trakt watchlist",
    labelnames=["media_type", "year", "status"],
    registry=_REGISTRY,
)


class Context:
    session: requests.Session
    data_dir: Path
    cache_dir: Path

    def __init__(
        self,
        session: requests.Session,
        data_dir: Path,
        cache_dir: Path,
    ) -> None:
        self.session = session
        self.data_dir = data_dir
        self.cache_dir = cache_dir


def _partition_filename(basedir: Path, id: int, suffix: str) -> Path:
    id_str = str(id)
    if len(id_str) == 1:
        id_prefix = id_str + "0"
    else:
        id_prefix = id_str[:2]
    return basedir / id_prefix / f"{id}{suffix}"


def _load_show_info(ctx: Context, trakt_id: int) -> Show | None:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "shows",
        id=trakt_id,
        suffix=".json",
    )
    if not output_path.exists():
        logger.warning("Show info not cached: %s", trakt_id)
        return None

    extended_show = read_json_data(output_path, ShowExtended)
    show: Show = {
        "title": extended_show["title"],
        "year": extended_show["year"],
        "ids": extended_show["ids"],
    }
    return show


def _export_media_season(
    ctx: Context,
    show_trakt_id: int,
    season_trakt_id: int,
    season_number: int,
) -> SeasonExtended:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "seasons",
        id=season_trakt_id,
        suffix=".json",
    )
    if output_path.exists():
        return read_json_data(output_path, SeasonExtended)

    data = trakt_api_get(
        ctx.session,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}/info",
        params={"extended": "full"},
    )
    episodes_data = trakt_api_get(
        ctx.session,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}",
    )
    data["show"] = _load_show_info(ctx, show_trakt_id)
    data["episodes"] = episodes_data
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    write_json(output_path, data, mtime=mtime)
    return cast(SeasonExtended, data)


def _export_media_show(
    ctx: Context,
    trakt_id: int,
    ignore_cache: bool = False,
) -> ShowExtended:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "shows",
        id=trakt_id,
        suffix=".json",
    )
    if output_path.exists() and not ignore_cache:
        return read_json_data(output_path, ShowExtended)

    data = trakt_api_get(
        ctx.session,
        path=f"/shows/{trakt_id}",
        params={"extended": "full"},
    )
    seasons_data = trakt_api_get(ctx.session, path=f"/shows/{trakt_id}/seasons")
    data["seasons"] = seasons_data
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    write_json(output_path, data, mtime=mtime)
    return cast(ShowExtended, data)


def _resolve_season_trakt_id(
    ctx: Context,
    show_trakt_id: int,
    season_number: int,
) -> int | None:
    show = _export_media_show(ctx, show_trakt_id)

    season_trakt_id: int | None = None
    for season in show["seasons"]:
        if season["number"] == season_number:
            season_trakt_id = season["ids"]["trakt"]
            break

    if season_trakt_id is None:
        logger.debug(
            "Invalid cache for '%s' S%d, re-fetching",
            show["title"],
            season_number,
        )
        show = _export_media_show(ctx, show_trakt_id, ignore_cache=True)

    for season in show["seasons"]:
        if season["number"] == season_number:
            season_trakt_id = season["ids"]["trakt"]
            break

    if season_trakt_id is None:
        logger.warning(
            "'%s' missing S%d",
            show["title"],
            season_number,
        )
        return None

    return season_trakt_id


def _resolve_episode_trakt_id(
    ctx: Context,
    show_trakt_id: int,
    season_number: int,
    episode_number: int,
) -> int | None:
    season_trakt_id = _resolve_season_trakt_id(
        ctx,
        show_trakt_id=show_trakt_id,
        season_number=season_number,
    )
    if season_trakt_id is None:
        return None

    season = _export_media_season(
        ctx,
        show_trakt_id=show_trakt_id,
        season_trakt_id=season_trakt_id,
        season_number=season_number,
    )

    episode_trakt_id: int | None = None
    for episode in season["episodes"]:
        if episode["number"] == episode_number:
            episode_trakt_id = episode["ids"]["trakt"]
            break

    if episode_trakt_id is None:
        show = _export_media_show(ctx, show_trakt_id)
        logger.warning(
            "'%s' missing S%dE%d",
            show["title"],
            season_number,
            episode_number,
        )
        return None

    return episode_trakt_id


def _export_media_episode(
    ctx: Context,
    episode_trakt_id: int | None,
    show_trakt_id: int,
    season_number: int,
    episode_number: int,
) -> EpisodeExtended:
    if not episode_trakt_id:
        episode_trakt_id = _resolve_episode_trakt_id(
            ctx,
            show_trakt_id=show_trakt_id,
            season_number=season_number,
            episode_number=episode_number,
        )

    if episode_trakt_id:
        output_path = _partition_filename(
            basedir=ctx.cache_dir / "media" / "episodes",
            id=episode_trakt_id,
            suffix=".json",
        )
        if output_path.exists():
            return read_json_data(output_path, EpisodeExtended)

    data = trakt_api_get(
        ctx.session,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}/episodes/{episode_number}",
        params={"extended": "full"},
    )
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "episodes",
        id=data["ids"]["trakt"],
        suffix=".json",
    )
    write_json(output_path, data, mtime=mtime)
    return cast(EpisodeExtended, data)


def _export_media_movie(ctx: Context, trakt_id: int) -> MovieExtended:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "movies",
        id=trakt_id,
        suffix=".json",
    )
    if output_path.exists():
        return read_json_data(output_path, MovieExtended)

    data = trakt_api_get(
        ctx.session,
        path=f"/movies/{trakt_id}",
        params={"extended": "full"},
    )
    releases = trakt_api_get(ctx.session, path=f"/movies/{trakt_id}/releases/us")
    data["releases"] = releases
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    write_json(output_path, data, mtime=mtime)
    return cast(MovieExtended, data)


@dataclass
class MetricInfo:
    type: Literal["movie", "show", "episode"]
    status: str
    year: str
    runtime: int


_FUTURE_YEAR = 3000


def _movie_release_status(movie: MovieExtended) -> MovieReleaseType:
    type_indices = set([0])
    for release in movie.get("releases", []):
        rd = datetime.fromisoformat(release["release_date"])
        if rd > datetime.now():
            continue
        try:
            type_index = MOVIE_RELEASE_TYPES.index(release["release_type"])
        except ValueError:
            logger.warning("Unknown release type: '%s'", release["release_type"])
            continue
        type_indices.add(type_index)

    type_index = max(type_indices)
    assert 0 <= type_index < len(MOVIE_RELEASE_TYPES)
    return MOVIE_RELEASE_TYPES[type_index]


def _fetch_movie_metric_info(ctx: Context, trakt_id: int) -> MetricInfo:
    movie = _export_media_movie(ctx, trakt_id=trakt_id)

    status = "unknown"
    release_status = _movie_release_status(movie)

    if movie["status"]:
        status = movie["status"]

    if status == "released":
        status = f"released/{release_status}"
    elif release_status != "unknown":
        logger.warning(
            "Movie status was '%s' but had a release status of '%s'",
            status,
            release_status,
        )

    year: str = str(_FUTURE_YEAR)
    if movie["year"]:
        year = str(movie["year"])

    runtime: int = 0
    if movie["runtime"]:
        runtime = movie["runtime"]

    return MetricInfo(type="movie", status=status, year=year, runtime=runtime)


def _fetch_show_metric_info(ctx: Context, trakt_id: int) -> MetricInfo:
    show = _export_media_show(ctx, trakt_id=trakt_id)

    status = "unknown"
    if show["status"]:
        status = show["status"]

    year: str = str(_FUTURE_YEAR)
    if show["year"]:
        year = str(show["year"])

    runtime: int = 0
    if show["runtime"] and show["aired_episodes"]:
        runtime = show["runtime"] * show["aired_episodes"]

    return MetricInfo(type="show", status=status, year=year, runtime=runtime)


def _fetch_episode_metric_info(
    ctx: Context,
    show_trakt_id: int,
    episode_trakt_id: int | None,
    season_number: int,
    episode_number: int,
) -> MetricInfo:
    show = _export_media_show(ctx, show_trakt_id)
    episode = _export_media_episode(
        ctx,
        show_trakt_id=show_trakt_id,
        episode_trakt_id=episode_trakt_id,
        season_number=season_number,
        episode_number=episode_number,
    )

    status = "unknown"
    if show["status"]:
        status = show["status"]

    year: str = str(_FUTURE_YEAR)
    if show["year"]:
        year = str(show["year"])
    if episode["first_aired"]:
        year = str(int(episode["first_aired"].split("-")[0]))

    runtime: int = 0
    if episode["runtime"]:
        runtime = episode["runtime"]

    return MetricInfo(type="episode", status=status, year=year, runtime=runtime)


def _generate_collection_metrics(ctx: Context, data_path: Path) -> None:
    movies_collection = read_json_data(
        data_path / "collection" / "collection-movies.json", list[CollectedMovie]
    )
    for collected_movie in movies_collection:
        trakt_id = collected_movie["movie"]["ids"]["trakt"]
        info = _fetch_movie_metric_info(ctx, trakt_id)
        _TRAKT_COLLECTION_COUNT.labels(
            media_type="movie",
            year=info.year,
        ).inc()

    shows_collection = read_json_data(
        data_path / "collection" / "collection-shows.json", list[CollectedShow]
    )
    for collected_show in shows_collection:
        show_trakt_id = collected_show["show"]["ids"]["trakt"]
        info = _fetch_show_metric_info(ctx, trakt_id=show_trakt_id)
        _TRAKT_COLLECTION_COUNT.labels(
            media_type="show",
            year=info.year,
        ).inc()

        for collected_season in collected_show["seasons"]:
            for collected_episode in collected_season["episodes"]:
                info2 = _fetch_episode_metric_info(
                    ctx,
                    show_trakt_id=show_trakt_id,
                    episode_trakt_id=None,
                    season_number=collected_season["number"],
                    episode_number=collected_episode["number"],
                )
                if info2:
                    _TRAKT_COLLECTION_COUNT.labels(
                        media_type="episode",
                        year=info2.year,
                    ).inc()


def _generate_ratings_metrics(ctx: Context, data_path: Path) -> None:
    episode_ratings = read_json_data(
        data_path / "ratings" / "ratings-episodes.json", list[EpisodeRating]
    )
    for episode_rating in episode_ratings:
        info = _fetch_episode_metric_info(
            ctx,
            show_trakt_id=episode_rating["show"]["ids"]["trakt"],
            episode_trakt_id=episode_rating["episode"]["ids"]["trakt"],
            season_number=episode_rating["episode"]["season"],
            episode_number=episode_rating["episode"]["number"],
        )
        _TRAKT_RATINGS_COUNT.labels(
            media_type="episode",
            year=info.year,
            rating=str(episode_rating["rating"]),
        ).inc()

    movie_ratings = read_json_data(
        data_path / "ratings" / "ratings-movies.json", list[MovieRating]
    )
    for movie_rating in movie_ratings:
        info = _fetch_movie_metric_info(ctx, movie_rating["movie"]["ids"]["trakt"])
        _TRAKT_RATINGS_COUNT.labels(
            media_type="movie",
            year=info.year,
            rating=str(movie_rating["rating"]),
        ).inc()

    show_ratings = read_json_data(
        data_path / "ratings" / "ratings-shows.json", list[ShowRating]
    )
    for show_rating in show_ratings:
        info = _fetch_show_metric_info(ctx, show_rating["show"]["ids"]["trakt"])
        _TRAKT_RATINGS_COUNT.labels(
            media_type="show",
            year=info.year,
            rating=str(show_rating["rating"]),
        ).inc()


def _generate_watched_metrics(ctx: Context, data_path: Path) -> None:
    history_items = read_json_data(
        data_path / "watched" / "history.json", list[HistoryItem]
    )
    for history_item in history_items:
        if history_item["type"] == "movie":
            info = _fetch_movie_metric_info(ctx, history_item["movie"]["ids"]["trakt"])
        elif history_item["type"] == "episode":
            info = _fetch_episode_metric_info(
                ctx,
                show_trakt_id=history_item["show"]["ids"]["trakt"],
                episode_trakt_id=history_item["episode"]["ids"]["trakt"],
                season_number=history_item["episode"]["season"],
                episode_number=history_item["episode"]["number"],
            )
        else:
            info = None
            logger.warning("Unknown media type: %s", history_item["type"])
            continue

        _TRAKT_WATCHED_COUNT.labels(
            media_type=info.type,
            year=info.year,
        ).inc()
        _TRAKT_WATCHED_MINUTES.labels(
            media_type=info.type,
            year=info.year,
        ).inc(info.runtime)


def _generate_list_metrics(ctx: Context, data_path: Path) -> None:
    lists = read_json_data(data_path / "lists" / "lists.json", list[List])

    for lst in lists:
        list_filename = f"list-{lst['ids']['trakt']}-{lst['ids']['slug']}.json"
        list_path = data_path / "lists" / list_filename
        list_items = read_json_data(list_path, list[ListItem])
        for item in list_items:
            if item["type"] == "movie":
                info = _fetch_movie_metric_info(ctx, item["movie"]["ids"]["trakt"])
            elif item["type"] == "show":
                info = _fetch_show_metric_info(ctx, item["show"]["ids"]["trakt"])
            else:
                info = None
                logger.warning("Unknown media type: %s", item["type"])
                continue

            _TRAKT_LIST_COUNT.labels(
                list=lst["ids"]["slug"],
                media_type=info.type,
                year=info.year,
                status=info.status,
            ).inc()
            _TRAKT_LIST_MINUTES.labels(
                list=lst["ids"]["slug"],
                media_type=info.type,
                year=info.year,
                status=info.status,
            ).inc(info.runtime)


def _generate_watchlist_metrics(ctx: Context, data_path: Path) -> None:
    watchlist = read_json_data(data_path / "lists" / "watchlist.json", list[ListItem])
    for item in watchlist:
        if item["type"] == "movie":
            info = _fetch_movie_metric_info(ctx, item["movie"]["ids"]["trakt"])
        elif item["type"] == "show":
            info = _fetch_show_metric_info(ctx, item["show"]["ids"]["trakt"])
        elif item["type"] == "episode":
            info = _fetch_episode_metric_info(
                ctx,
                show_trakt_id=item["show"]["ids"]["trakt"],
                episode_trakt_id=item["episode"]["ids"]["trakt"],
                season_number=item["episode"]["season"],
                episode_number=item["episode"]["number"],
            )
        else:
            info = None
            logger.warning("Unknown media type: %s", item["type"])
            continue

        _TRAKT_WATCHLIST_COUNT.labels(
            media_type=info.type,
            year=info.year,
            status=info.status,
        ).inc()
        _TRAKT_WATCHLIST_MINUTES.labels(
            media_type=info.type,
            year=info.year,
            status=info.status,
        ).inc(info.runtime)


def generate_metrics(
    session: requests.Session,
    data_dir: Path,
    cache_dir: Path,
) -> None:
    ctx = Context(
        session=session,
        data_dir=data_dir,
        cache_dir=cache_dir,
    )

    user_profile = read_json_data(
        data_dir / "user" / "profile.json",
        UserProfile,
    )
    username = user_profile["username"]

    _TRAKT_VIP_YEARS.labels(username=username).set(user_profile["vip_years"])

    _generate_collection_metrics(ctx, data_dir)
    _generate_ratings_metrics(ctx, data_dir)
    _generate_list_metrics(ctx, data_dir)
    _generate_watched_metrics(ctx, data_dir)
    _generate_watchlist_metrics(ctx, data_dir)

    metrics_path: str = str(data_dir / "metrics.prom")
    write_to_textfile(metrics_path, _REGISTRY)
