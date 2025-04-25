from datetime import datetime
from pathlib import Path
from typing import cast

import requests
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

from . import logger
from .export import read_json_data, write_json
from .trakt import (
    CollectedMovie,
    CollectedShow,
    EpisodeExtended,
    EpisodeRating,
    HistoryItem,
    ListItem,
    MovieExtended,
    MovieRating,
    SeasonExtended,
    ShowExtended,
    ShowRating,
    UserProfile,
    trakt_api_get,
)

_FUTURE_YEAR = 3000

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


def _episode_first_aired_year(
    episode: EpisodeExtended, show: ShowExtended
) -> int | None:
    year = show.get("year")
    if episode.get("first_aired"):
        year = int(episode["first_aired"].split("-")[0])
    return year


def _partition_filename(basedir: Path, id: int, suffix: str) -> Path:
    id_str = str(id)
    if len(id_str) == 1:
        id_prefix = id_str + "0"
    else:
        id_prefix = id_str[:2]
    return basedir / id_prefix / f"{id}{suffix}"


def _fetch_media_episode(
    ctx: Context,
    show_trakt_id: int,
    season_number: int,
    episode_number: int,
) -> EpisodeExtended:
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


def _load_cached_media_episode(
    ctx: Context,
    trakt_id: int,
) -> EpisodeExtended | None:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "episodes",
        id=trakt_id,
        suffix=".json",
    )
    if output_path.exists():
        return read_json_data(output_path, EpisodeExtended)
    return None


def _export_media_episode2(
    ctx: Context,
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
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    write_json(output_path, data, mtime=mtime)
    return cast(MovieExtended, data)


def _load_cached_media_season(
    ctx: Context,
    trakt_id: int,
) -> SeasonExtended | None:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "seasons",
        id=trakt_id,
        suffix=".json",
    )
    if output_path.exists():
        return read_json_data(output_path, SeasonExtended)
    return None


def _fetch_media_season(
    ctx: Context,
    show_trakt_id: int,
    season_number: int,
) -> SeasonExtended:
    data = trakt_api_get(
        ctx.session,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}/info",
        params={"extended": "full"},
    )
    episodes_data = trakt_api_get(
        ctx.session,
        path=f"/shows/{show_trakt_id}/seasons/{season_number}",
    )
    data["episodes"] = episodes_data
    mtime = datetime.fromisoformat(data["updated_at"]).timestamp()
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "seasons",
        id=data["ids"]["trakt"],
        suffix=".json",
    )
    write_json(output_path, data, mtime=mtime)
    return cast(SeasonExtended, data)


def _export_media_season2(
    ctx: Context,
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


def _export_media_show(ctx: Context, trakt_id: int) -> ShowExtended:
    output_path = _partition_filename(
        basedir=ctx.cache_dir / "media" / "shows",
        id=trakt_id,
        suffix=".json",
    )

    if output_path.exists():
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


def _generate_collection_metrics(ctx: Context, data_path: Path) -> None:
    movies_collection = read_json_data(
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

    shows_collection = read_json_data(
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


def _export_media_episode(
    ctx: Context,
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


def _export_media_season(
    ctx: Context,
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


def _generate_ratings_metrics(ctx: Context, data_path: Path) -> None:
    episode_ratings = read_json_data(
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

    movie_ratings = read_json_data(
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

    show_ratings = read_json_data(
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


def _generate_watched_metrics(ctx: Context, data_path: Path) -> None:
    history_items = read_json_data(
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
            show = _export_media_show(
                ctx,
                trakt_id=history_item["show"]["ids"]["trakt"],
            )
            episode = _export_media_episode(
                ctx,
                trakt_id=history_item["episode"]["ids"]["trakt"],
                show_trakt_id=history_item["show"]["ids"]["trakt"],
                season=history_item["episode"]["season"],
                number=history_item["episode"]["number"],
            )
            year_str = str(_episode_first_aired_year(episode, show) or _FUTURE_YEAR)
            runtime = episode["runtime"]
            _TRAKT_WATCHED_COUNT.labels(
                media_type="episode",
                year=year_str,
            ).inc()
            _TRAKT_WATCHED_RUNTIME.labels(
                media_type="episode",
                year=year_str,
            ).inc(runtime)


def _compute_show_approx_runtime(show: ShowExtended) -> int:
    return show["runtime"] * show["aired_episodes"]


def _generate_watchlist_metrics(ctx: Context, data_path: Path) -> None:
    watchlist = read_json_data(data_path / "lists" / "watchlist.json", list[ListItem])
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
            runtime = _compute_show_approx_runtime(show)

            _TRAKT_WATCHLIST_COUNT.labels(
                media_type="show",
                status=status,
                year=year_str,
            ).inc()
            _TRAKT_WATCHLIST_RUNTIME.labels(
                media_type="show",
                status=status,
                year=year_str,
            ).inc(runtime)
        else:
            logger.warning("Unknown media type: %s", item["type"])


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
    _generate_watched_metrics(ctx, data_dir)
    _generate_watchlist_metrics(ctx, data_dir)

    metrics_path: str = str(data_dir / "metrics.prom")
    write_to_textfile(metrics_path, _REGISTRY)
