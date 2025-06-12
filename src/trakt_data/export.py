import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

import requests

from . import logger
from .trakt import (
    HiddenShow,
    HistoryItem,
    LastActivities,
    List,
    ProgressShow,
    UpNextShow,
    WatchedShow,
    trakt_api_get,
    trakt_api_paginated_get,
)


class Context:
    session: requests.Session
    output_dir: Path
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
        self.session = session
        self.output_dir = output_dir
        self.exclude_paths = exclude_paths
        self.fresh_paths = fresh_paths
        self.stale_paths = stale_paths


def _excluded(ctx: Context, path: Path) -> bool:
    for excluded_path in ctx.exclude_paths:
        if path == excluded_path:
            return True
        elif path.is_relative_to(excluded_path):
            return True
    return False


def _fresh(ctx: Context, path: Path) -> bool:
    if not path.exists():
        return False
    elif path in ctx.fresh_paths:
        return True
    elif path in ctx.stale_paths:
        return False
    else:
        logger.warning("Path freshness is unknown: %s", path)
        return False


def write_json(path: Path, obj: Any, mtime: float | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, indent=2)
    path.write_text(data + "\n")
    if mtime:
        path.touch()
        os.utime(path, (mtime, mtime))


def _export_user_profile(ctx: Context) -> None:
    output_path = ctx.output_dir / "user" / "profile.json"

    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return

    data = trakt_api_get(ctx.session, path="/users/me", params={"extended": "vip"})

    profile = {
        "username": data["username"],
        "name": data["name"],
        "vip": data["vip"],
        "vip_ep": data["vip_ep"],
        "ids": data["ids"],
        "vip_og": data["vip_og"],
        "vip_years": data["vip_years"],
    }
    write_json(output_path, profile)


def _export_user_stats(ctx: Context) -> None:
    output_path = ctx.output_dir / "user" / "stats.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_get(ctx.session, path="/users/me/stats")
    write_json(output_path, data)


T = TypeVar("T")


def read_json_data(path: Path, return_type: type[T]) -> T:
    return cast(T, json.loads(path.read_text()))


def _export_watched_history(ctx: Context) -> None:
    output_path = ctx.output_dir / "watched" / "history.json"

    if _fresh(ctx, output_path):
        return

    if output_path.exists():
        existing_items = read_json_data(output_path, list[HistoryItem])
        start_at = existing_items[0]["watched_at"]

        new_items = trakt_api_paginated_get(
            ctx.session,
            path="/sync/history",
            params={"start_at": start_at},
        )
        if len(new_items) <= 1:
            logger.info("No new items watched since %s", start_at)
            return

    data = trakt_api_paginated_get(ctx.session, path="/sync/history")
    write_json(output_path, data)


def _export_watched_playback(ctx: Context) -> None:
    output_path = ctx.output_dir / "watched" / "playback.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_get(ctx.session, path="/sync/playback")
    write_json(output_path, data)


def _export_watched(
    ctx: Context,
    type: Literal["movies", "shows"],
) -> None:
    output_path = ctx.output_dir / "watched" / f"watched-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_get(ctx.session, path=f"/sync/watched/{type}")
    write_json(output_path, data)


def _export_collection(
    ctx: Context,
    type: Literal["movies", "shows"],
) -> None:
    output_path = ctx.output_dir / "collection" / f"collection-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_get(ctx.session, path=f"/sync/collection/{type}")
    write_json(output_path, data)


def _export_comments(
    ctx: Context,
    type: Literal["movies", "shows", "seasons", "episodes", "lists"],
) -> None:
    output_path = ctx.output_dir / "comments" / f"comments-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_paginated_get(ctx.session, path=f"/users/me/comments/{type}")
    write_json(output_path, data)


def _export_hidden(ctx: Context, section: str) -> None:
    output_path = ctx.output_dir / "hidden" / f"hidden-{section.replace('_', '-')}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_paginated_get(ctx.session, path=f"/users/hidden/{section}")
    write_json(output_path, data)


def _export_likes(
    ctx: Context,
    type: Literal["comments", "lists"],
) -> None:
    output_path = ctx.output_dir / "likes" / f"likes-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_paginated_get(ctx.session, path=f"/users/me/likes/{type}")
    write_json(output_path, data)


def _export_lists_list(ctx: Context, list_id: int, list_slug: str) -> None:
    output_path = ctx.output_dir / "lists" / f"list-{list_id}-{list_slug}.json"

    if _excluded(ctx, output_path):
        return
    elif output_path.exists() and output_path in ctx.fresh_paths:
        return

    data = trakt_api_paginated_get(ctx.session, path=f"/users/me/lists/{list_id}/items")
    write_json(output_path, data)


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

    if _excluded(ctx, output_path):
        return

    if _fresh(ctx, output_path):
        return

    data = trakt_api_get(ctx.session, path="/users/me/lists")
    write_json(output_path, data)
    _export_lists_list_all(ctx, cast(list[List], data))


def _export_lists_watchlist(ctx: Context) -> None:
    output_path = ctx.output_dir / "lists" / "watchlist.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_paginated_get(
        ctx.session,
        path="/sync/watchlist",
        params={"sort_by": "rank", "sort_how": "asc"},
    )
    write_json(output_path, data)


def _export_ratings(
    ctx: Context,
    type: Literal["movies", "shows", "seasons", "episodes"],
) -> None:
    output_path = ctx.output_dir / "ratings" / f"ratings-{type}.json"
    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return
    data = trakt_api_get(ctx.session, path=f"/sync/ratings/{type}")
    write_json(output_path, data)


def _export_shows_watched_progress(ctx: Context) -> None:
    output_path = ctx.output_dir / "watched" / "progress-shows.json"

    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return

    watched_shows = read_json_data(
        ctx.output_dir / "watched" / "watched-shows.json",
        list[WatchedShow],
    )

    shows: list[Any] = []
    for watched_show in watched_shows:
        show_id = watched_show["show"]["ids"]["trakt"]
        path = f"/shows/{show_id}/progress/watched"
        data = trakt_api_get(ctx.session, path=path)
        show_progress = {
            "show": watched_show["show"],
            "progress": data,
        }
        shows.append(show_progress)

    write_json(output_path, shows)


def _export_user_last_activities(ctx: Context) -> LastActivities:
    output_path = ctx.output_dir / "user" / "last-activities.json"
    data = trakt_api_get(ctx.session, path="/sync/last_activities")
    write_json(output_path, data)
    return cast(LastActivities, data)


def _last_hidden_at_activities(activities: LastActivities) -> datetime:
    return max(
        datetime.fromisoformat(activities["movies"]["hidden_at"]),
        datetime.fromisoformat(activities["shows"]["hidden_at"]),
        datetime.fromisoformat(activities["seasons"]["hidden_at"]),
    )


def _last_dropped_at_activities(activities: LastActivities) -> datetime:
    return datetime.fromisoformat(activities["shows"]["dropped_at"])


def _last_watched_at_activities(activities: LastActivities) -> datetime:
    return max(
        datetime.fromisoformat(activities["movies"]["watched_at"]),
        datetime.fromisoformat(activities["episodes"]["watched_at"]),
    )


def _last_paused_at_activities(activities: LastActivities) -> datetime:
    return max(
        datetime.fromisoformat(activities["movies"]["paused_at"]),
        datetime.fromisoformat(activities["episodes"]["paused_at"]),
    )


def _compare_datetime_strs(a: str, b: str) -> bool:
    return datetime.fromisoformat(a) >= datetime.fromisoformat(b)


def _export_shows_up_next(ctx: Context) -> None:
    output_path = ctx.output_dir / "watched" / "up-next.json"

    if _excluded(ctx, output_path) or _fresh(ctx, output_path):
        return

    watched_shows = read_json_data(
        ctx.output_dir / "watched" / "watched-shows.json",
        list[WatchedShow],
    )
    watched_show_ids: dict[int, WatchedShow] = {}
    for watched_show in watched_shows:
        watched_show_ids[watched_show["show"]["ids"]["trakt"]] = watched_show

    hidden_show_trakt_ids: set[int] = set()
    dropped_shows = read_json_data(
        ctx.output_dir / "hidden" / "hidden-dropped.json",
        list[HiddenShow],
    )
    for hidden_show in dropped_shows:
        hidden_show_trakt_ids.add(hidden_show["show"]["ids"]["trakt"])
    hidden_progress_watched_shows = read_json_data(
        ctx.output_dir / "hidden" / "hidden-progress-watched.json",
        list[HiddenShow],
    )
    for hidden_show in hidden_progress_watched_shows:
        hidden_show_trakt_ids.add(hidden_show["show"]["ids"]["trakt"])

    watched_show_progresses = read_json_data(
        ctx.output_dir / "watched" / "progress-shows.json",
        list[ProgressShow],
    )

    up_next_shows: list[UpNextShow] = []
    for progress_show in watched_show_progresses:
        trakt_show_id = progress_show["show"]["ids"]["trakt"]
        show_title = progress_show["show"]["title"]
        show_watch = watched_show_ids[trakt_show_id]
        show_progress = progress_show["progress"]

        if trakt_show_id in hidden_show_trakt_ids:
            logger.debug("Skipping hidden show: %s", show_title)
            continue

        if show_progress["aired"] == show_progress["completed"]:
            logger.debug("Skipping show with all episodes completed: %s", show_title)
            continue

        next_episode = show_progress["next_episode"]
        if not next_episode:
            logger.debug("Skipping show with no next episode: %s", show_title)
            continue

        up_next_show: UpNextShow = {
            "show": progress_show["show"],
            "progress": {
                "aired": show_progress["aired"],
                "completed": show_progress["completed"],
                "hidden": 0,  # TODO
                "last_watched_at": show_progress["last_watched_at"],
                "reset_at": show_progress["reset_at"],
                "stats": {
                    "play_count": show_watch["plays"],
                    "minutes_left": 0,  # TODO
                    "minutes_watched": 0,  # TODO
                },
                "next_episode": next_episode,
                "last_episode": show_progress["last_episode"],
            },
        }
        up_next_shows.append(up_next_show)

    write_json(output_path, up_next_shows)


def _activities_outdated_paths(
    data_path: Path,
    old_activities: LastActivities | None,
    new_activities: LastActivities,
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
        ("episodes", "watched_at", data_path / "watched" / "up-next.json"),
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

    dropped_at_fresh = False
    if old_activities:
        dropped_at_fresh = _last_dropped_at_activities(
            new_activities
        ) >= _last_dropped_at_activities(old_activities)
    _mark_path(data_path / "hidden" / "hidden-dropped.json", dropped_at_fresh)

    old_activities_hidden_at = datetime.fromtimestamp(0, tz=timezone.utc)
    if old_activities:
        old_activities_hidden_at = _last_hidden_at_activities(old_activities)
    new_activities_hidden_at = _last_hidden_at_activities(new_activities)
    hidden_at_fresh = old_activities_hidden_at >= new_activities_hidden_at
    _mark_path(data_path / "hidden" / "hidden-calendar.json", hidden_at_fresh)
    _mark_path(data_path / "hidden" / "hidden-progress-collected.json", hidden_at_fresh)
    _mark_path(
        data_path / "hidden" / "hidden-progress-watched-reset.json", hidden_at_fresh
    )
    _mark_path(data_path / "hidden" / "hidden-progress-watched.json", hidden_at_fresh)
    _mark_path(data_path / "hidden" / "hidden-recommendations.json", hidden_at_fresh)

    return (fresh_paths, stale_paths)


def export_all(
    session: requests.Session,
    output_dir: Path,
    exclude: list[str] = [],
) -> None:
    exclude_paths: list[Path] = []
    for path in exclude:
        if path.startswith(".") or path.startswith("/"):
            exclude_paths.append(Path(path))
        else:
            exclude_paths.append(output_dir / path)

    logger.debug("exclude_paths: %s", exclude_paths)

    ctx = Context(
        session=session,
        output_dir=output_dir,
        exclude_paths=exclude_paths,
        fresh_paths=[],
        stale_paths=[],
    )

    old_activities: LastActivities | None = None
    if (output_dir / "user" / "last-activities.json").exists():
        old_activities = read_json_data(
            output_dir / "user" / "last-activities.json",
            LastActivities,
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
    logger.debug("stale_paths: %s", stale_paths)

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
    _export_shows_up_next(ctx)
