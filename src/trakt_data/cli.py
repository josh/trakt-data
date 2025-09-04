import logging
import os
from datetime import timedelta
from pathlib import Path

import click

from .cache import (
    default_cache_dir,
    fix_cache_mtimes,
    print_cache_stats,
    prune_cache_dir,
)
from .export import export_all
from .metrics import generate_metrics
from .trakt import trakt_session


def _configure_logger(verbose: bool) -> None:
    in_gha = os.getenv("GITHUB_ACTIONS") == "true"
    gh_fmt = "::%(levelname)s file=%(pathname)s,line=%(lineno)d::%(message)s"
    default_fmt = "%(levelname)s %(name)s:%(lineno)d %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=gh_fmt if in_gha else default_fmt,
        datefmt="%Y-%m-%d %H:%M",
        force=True,
    )
    logging.addLevelName(logging.ERROR, "error")
    logging.addLevelName(logging.WARNING, "warning")
    logging.addLevelName(logging.INFO, "notice")
    logging.addLevelName(logging.DEBUG, "debug")


@click.group()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
@click.version_option()
def main(verbose: bool) -> None:
    _configure_logger(verbose)


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
    session = trakt_session(
        client_id=trakt_client_id,
        access_token=trakt_access_token,
    )
    export_all(
        session=session,
        output_dir=output_dir,
        exclude=exclude,
    )


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
    default=default_cache_dir(),
    show_default=True,
)
def metrics(
    trakt_client_id: str,
    trakt_access_token: str,
    output_dir: Path,
    cache_dir: Path,
) -> None:
    session = trakt_session(
        client_id=trakt_client_id,
        access_token=trakt_access_token,
    )
    generate_metrics(
        session=session,
        data_dir=output_dir,
        cache_dir=cache_dir,
    )


def _parse_timedelta(value: str) -> timedelta:
    if value == "" or value == "0":
        return timedelta(days=0)
    elif value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Invalid time delta: {value}")


def _parse_limit(value: str) -> int | float:
    if value.endswith("%"):
        return float(value[:-1]) / 100
    return int(value)


@main.command()
@click.option(
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=default_cache_dir(),
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
    prune_cache_dir(
        cache_dir=cache_dir,
        min_age=_parse_timedelta(min_age),
        limit=_parse_limit(limit),
        dry_run=dry_run,
    )


@main.command()
@click.option(
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=default_cache_dir(),
    show_default=True,
)
def cache_stats(cache_dir: Path) -> None:
    print_cache_stats(cache_dir=cache_dir)


@main.command()
@click.option(
    "--cache-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    default=default_cache_dir(),
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
    fix_cache_mtimes(
        cache_dir=cache_dir,
        dry_run=dry_run,
    )
