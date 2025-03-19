import json
import logging
from pathlib import Path
from typing import Any, TypedDict

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

    profile = _export_user_profile(_session)

    _write_json(output_dir / "user" / "profile.json", profile)

    _generate_metrics(data_path=output_dir)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, indent=2)
    path.write_text(data)


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


def _export_user_profile(session: requests.Session) -> ExportUserProfile:
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


def _read_json_data(path: Path) -> Any:
    return json.loads(path.read_text())


registry = CollectorRegistry()

trakt_vip_years = Gauge(
    "trakt_vip_years",
    documentation="Trakt VIP years",
    labelnames=["username"],
    registry=registry,
)


def _generate_metrics(data_path: Path) -> None:
    user_profile: ExportUserProfile = _read_json_data(
        data_path / "user" / "profile.json"
    )
    username = user_profile["username"]

    trakt_vip_years.labels(username=username).set(user_profile["vip_years"])

    metrics_path: str = str(data_path / "metrics.prom")
    write_to_textfile(metrics_path, registry)


if __name__ == "__main__":
    main()
