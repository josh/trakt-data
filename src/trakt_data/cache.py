import json
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from . import logger


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


def default_cache_dir() -> Path:
    return _xdg_cache_home() / "trakt-data"


def print_cache_stats(cache_dir: Path) -> None:
    now: float = time.time()
    ages = [now - file.stat().st_mtime for file in cache_dir.glob("**/*.json")]
    ages.sort()

    if not ages:
        print("Cache is empty")
        return

    mean_age = sum(ages) / len(ages)
    median_age = ages[len(ages) // 2]
    p75_age = ages[int(len(ages) * 0.75)]
    p95_age = ages[int(len(ages) * 0.95)]
    p99_age = ages[int(len(ages) * 0.99)]
    min_age = ages[0]
    max_age = ages[-1]

    print(f"mean age: {timedelta(seconds=int(mean_age))}")
    print(f"median age: {timedelta(seconds=int(median_age))}")
    print(f"75th percentile age: {timedelta(seconds=int(p75_age))}")
    print(f"95th percentile age: {timedelta(seconds=int(p95_age))}")
    print(f"99th percentile age: {timedelta(seconds=int(p99_age))}")
    print(f"min age: {timedelta(seconds=int(min_age))}")
    print(f"max age: {timedelta(seconds=int(max_age))}")
    print(f"total files: {len(ages)}")


def fix_cache_mtimes(cache_dir: Path, dry_run: bool = False) -> None:
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


def prune_cache_dir(
    cache_dir: Path,
    min_age: timedelta,
    limit: int | float,
    dry_run: bool,
) -> None:
    now = datetime.now()
    min_mtime: datetime = now - min_age
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

    limit_abs: int
    if isinstance(limit, float):
        assert 0 <= limit <= 1, f"Invalid percentage limit: {limit}"
        limit_abs = int(len(files) * limit)
        logger.debug("Using percentage limit: %s -> %s", limit, limit_abs)
    else:
        limit_abs = int(limit)
        logger.debug("Using absolute limit: %s", limit)

    indices = list(range(len(files)))
    random.shuffle(indices)
    expired_indices = indices[:limit_abs]

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
        logger.debug("Prune '%s' (%s, %s)", file, mtime, age_dt)
        if not dry_run:
            file.unlink()
