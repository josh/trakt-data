from typing import Any, Literal, TypedDict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import logger


class UserIDs(TypedDict):
    slug: str


class UserProfile(TypedDict):
    username: str
    name: str
    ids: UserIDs
    vip_years: int


class MovieIDs(TypedDict):
    trakt: int


class ShowIDs(TypedDict):
    trakt: int


class SeasonIDs(TypedDict):
    trakt: int


class EpisodeIDs(TypedDict):
    trakt: int


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
    status: str
    updated_at: str
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


MovieReleaseType = Literal[
    "unknown",
    "premiere",
    "limited",
    "theatrical",
    "digital",
    "physical",
    "tv",
]

MOVIE_RELEASE_TYPES: list[MovieReleaseType] = [
    "unknown",
    "premiere",
    "limited",
    "theatrical",
    "digital",
    "physical",
    "tv",
]


class MovieRelease(TypedDict):
    country: str
    certification: str
    release_date: str
    release_type: MovieReleaseType
    note: str | None


class MovieExtended(TypedDict):
    title: str
    year: int
    ids: MovieIDs
    released: str
    runtime: int
    status: str
    updated_at: str
    # Non-standard fields
    releases: list[MovieRelease]


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


class LastActivities(TypedDict):
    all: str
    movies: "MoviesLastActivities"
    episodes: "EpisodesLastActivities"
    shows: "ShowsLastActivities"
    seasons: "SeasonsLastActivities"


class MoviesLastActivities(TypedDict):
    watched_at: str
    collected_at: str
    rated_at: str
    watchlisted_at: str
    favorited_at: str
    recommendations_at: str
    commented_at: str
    paused_at: str
    hidden_at: str


class EpisodesLastActivities(TypedDict):
    watched_at: str
    collected_at: str
    rated_at: str
    watchlisted_at: str
    commented_at: str
    paused_at: str


class ShowsLastActivities(TypedDict):
    rated_at: str
    watchlisted_at: str
    favorited_at: str
    recommendations_at: str
    commented_at: str
    hidden_at: str
    dropped_at: str


class SeasonsLastActivities(TypedDict):
    rated_at: str
    watchlisted_at: str
    commented_at: str
    hidden_at: str


_TRAKT_API_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-key": "",
    "trakt-api-version": "2",
    "Authorization": "Bearer [access_token]",
}


def trakt_session(client_id: str, access_token: str) -> requests.Session:
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


def trakt_api_get(
    session: requests.Session,
    path: str,
    params: dict[str, str] = {},
) -> Any:
    if not path.startswith("/"):
        path = f"/{path}"

    logger.info("GET %s", f"https://api.trakt.tv{path}")
    response = session.get(f"https://api.trakt.tv{path}", params=params)
    response.raise_for_status()

    if "x-pagination-page" in response.headers:
        raise ValueError("Paginated response not supported")

    return response.json()


def trakt_api_paginated_get(
    session: requests.Session,
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
        response = session.get(f"https://api.trakt.tv{path}", params=params)
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
