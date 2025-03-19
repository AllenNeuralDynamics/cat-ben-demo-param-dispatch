# stdlib imports --------------------------------------------------- #
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import dataclasses
import datetime
import json
import functools
import logging
import logging.handlers
import os
import pathlib
import sys
import time
import types
import typing
import uuid
import zoneinfo
from typing import Any, Generator, Iterable, Literal

import polars as pl


logger = logging.getLogger(__name__)

CO_COMPUTATION_ID = os.environ.get("CO_COMPUTATION_ID")
AWS_BATCH_JOB_ID = os.environ.get("AWS_BATCH_JOB_ID")

def is_pipeline():
    return bool(AWS_BATCH_JOB_ID)

# logging ----------------------------------------------------------- #
class PSTFormatter(logging.Formatter):

    def converter(self, timestamp):
        # may require 'tzdata' package
        dt = datetime.datetime.fromtimestamp(timestamp, tz=zoneinfo.ZoneInfo("UTC"))
        return dt.astimezone(zoneinfo.ZoneInfo("US/Pacific"))

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            t = dt.strftime(self.default_time_format)
            s = self.default_msec_format % (t, record.msecs)
        return s

def setup_logging(
    level: int | str = logging.INFO, filepath: str | None = None
) -> logging.handlers.QueueListener | None:
    """
    Setup logging that works for local, capsule and pipeline environments.

    - with no input arguments, log messages at INFO level and above are printed to stdout
    - in Code Ocean capsules, stdout is captured in an 'output' file automatically
    - in pipelines, stdout from each capsule instance is also captured in a central 'output' file
      - for easier reading, this function saves log files from each capsule instance individually to logs/<AWS_BATCH_JOB_ID>.log
    - in local environments or capsules, file logging can be enabled by setting the `filepath` argument

    Note: logger is not currently safe for multiprocessing/threading (ignore WIP below)

    Note: if file logging is enabled in a multiprocessing/multithreading context, a `queue` should be set to True
    to correctly handle logs from multiple processes/threads. In this mode, a QueueListener is returned.
    When processes/threads shutdown, `QueueListener().stop()` must be called to ensure all logs are captured correctly.
    The `queue_logging()` context manager is provided to handle this within a process/thread:

        ```python
        def worker_process():
            with queue_logging():
                logger.info('Process started')
                # do work here
                logger.info('Process finished')

        processes = []
        for _ in range(5):
            process = multiprocessing.Process(target=worker_process)
            processes.append(process)
            process.start()

        for process in processes:
            process.join()
        logger.info('All processes finished')
        ```

    """
    if is_pipeline():
        assert AWS_BATCH_JOB_ID is not None
        co_prefix = f"{AWS_BATCH_JOB_ID.split('-')[0]}."
    else:
        co_prefix = ""

    fmt = f"%(asctime)s | %(levelname)s | {co_prefix}%(name)s.%(funcName)s | %(message)s"
    
    formatter = PSTFormatter( # use Seattle time
        fmt=fmt,
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    
    handlers: list[logging.Handler] = []
    
    # Create a console handler
    console_handler = logging.StreamHandler(stream=sys.stdout)
    handlers.append(console_handler)
    
    if is_pipeline() and not filepath:
        filepath = f"/results/logs/{AWS_BATCH_JOB_ID}_{int(time.time())}.log"
        # note: filename must be unique if we want to collect logs at end of pipeline
        
    if filepath:
        pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=filepath,
            maxBytes=1024 * 1024 * 10,
        )
        handlers.append(file_handler)

    # Apply formatting to the console handler and attach to root logger
    for handler in handlers:
        handler.setFormatter(formatter)
    # Configure the root logger
    logging.basicConfig(level=level, handlers=handlers)


# data access ------------------------------------------------------- #    

def get_df(component: str, lazy: bool = False) -> pl.DataFrame:
    path = get_datacube_dir() / 'consolidated' / f'{component}.parquet'
    if lazy:
        frame = pl.scan_parquet(path)
    else:
        frame = pl.read_parquet(path)
    return frame

# paths ----------------------------------------------------------- #

@functools.cache
def get_data_root(as_str: bool = False) -> pathlib.Path:
    expected_paths = ('/data', '/tmp/data', )
    for p in expected_paths:
        if (data_root := pathlib.Path(p)).exists():
            logger.debug(f"Using {data_root=}")
        return data_root.as_posix() if as_str else data_root
    else:
        raise FileNotFoundError(f"data dir not present at any of {expected_paths=}")

@functools.cache
def get_datacube_dir() -> pathlib.Path:
    for p in sorted(get_data_root().iterdir(), reverse=True): # in case we have multiple assets attached, the latest will be used
        if p.is_dir() and p.name.startswith('dynamicrouting_datacube'):
            path = p
            break
    else:
        for p in get_data_root().iterdir():
            if any(pattern in p.name for pattern in ('session_table', 'nwb', 'consolidated', )):
                path = get_data_root()
                break
        else:
            raise FileNotFoundError(f"Cannot determine datacube dir: {list(get_data_root().iterdir())=}")
    logger.debug(f"Using files in {path}")
    return path

@functools.cache
def get_nwb_paths() -> tuple[pathlib.Path, ...]:
    return tuple(get_data_root().rglob('*.nwb'))

def ensure_nonempty_results_dirs(dirs: str | Iterable[str] = '/results') -> None:
    """A pipeline run can crash if a results folder is expected and not found or is empty 
    - ensure that a non-empty folder exists by creating a unique file"""
    if not is_pipeline():
        return
    if isinstance(dirs, str):
        dirs = (dirs, )
    for d in dirs:
        results_dir = pathlib.Path(d)
        results_dir.mkdir(exist_ok=True)
        if not list(results_dir.iterdir()):
            path = results_dir / uuid.uuid4().hex
            logger.info(f"Creating {path} to ensure results folder is not empty")
            path.touch()
