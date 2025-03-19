# stdlib imports --------------------------------------------------- #
import argparse
import dataclasses
import json
import functools
import logging
import pathlib
import time
import types
import typing
import uuid
from typing import Any, Literal
import copy 

# 3rd-party imports necessary for processing ----------------------- #
import polars as pl
import pydantic_settings
import pydantic_settings.sources

# local modules ---------------------------------------------------- #
import utils

# logging configuration -------------------------------------------- #
# use `logger.info(msg)` instead of `print(msg)` so we get timestamps and origin of log messages
logger = logging.getLogger(
    pathlib.Path(__file__).stem if __name__.endswith("_main__") else __name__
    # multiprocessing gives name '__mp_main__'
)


# define capsule parameters here ----------------------------------------- #

"""
Normally, the values passed to init are used, combined with the default values:
    >>> CapsuleParameters(n_units_list=[0, 1])
    CapsuleParameters(n_units_list=[0, 1], logging_level='INFO', test=False)

With a `parameters.json` file containing `{"n_units_list": [25, 50, 100]}`, we don't need to pass
the `n_units_list` parameter to the constructor:
    >>> CapsuleParameters()
    CapsuleParameters(n_units_list=[25, 50, 100], logging_level='INFO', test=False)

And with `cli_parse_args=True`, values can also be input from the command line.

The order of the sources in `settings_customise_sources` determines the priority of the sources.

This allows us to combine inputs from multiple sources, particularly useful for running code in a
capsule that is standalone or part of a pipeline.
"""

class CapsuleParameters(pydantic_settings.BaseSettings):

    n_units_list: list[int]
    session_id: str | None = None
    logging_level: str | int = 'INFO'
    test: bool = False

    # set the priority of the sources:
    # ignore the function signature for now: concentrate on the return value
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        *args,
        **kwargs,
    ):
        # the order of the sources is what defines the priority:
        # - first source is highest priority
        # - for each field in the class, the first source that contains a value will be used
        return (
            init_settings,
            pydantic_settings.sources.JsonConfigSettingsSource(settings_cls, json_file='parameters.json'),
            pydantic_settings.CliSettingsSource(settings_cls, cli_parse_args=True),
        )


# processing function ---------------------------------------------- #
# modify the body of this function, but keep the same signature
def process_session(params: CapsuleParameters) -> None:
    """Process a single session with parameters defined in `app_params` and save results + app_params to
    /results.
    
    A test mode should be implemented to allow for quick testing of the capsule (required every time
    a change is made if the capsule is in a pipeline) 
    """
    logger.info(f"Processing {params!r}")
    
    # Process data here, with test mode implemented to break out of the loop early or use reduced param set:
    for idx, n_units in enumerate(params.n_units_list):

        # Save data to files in /results
        # If the same name is used across parallel runs of this capsule in a pipeline, a name clash will
        # occur and the pipeline will fail, so use params.session_id as filename prefix:
        #   /results/<sessionId>.suffix
        
        output_path = pathlib.Path('/results') / f'{params.session_id}_input_parameters_{idx}.json'
        logger.info(f"Writing {output_path}")

        if params.test:
            logger.info("TEST | Exiting after writing first parameter set")
            break
        
# ------------------------------------------------------------------ #


def main():
    t0 = time.time()
    
    utils.setup_logging()

    # get arguments passed from command line or "AppBuilder" interface:
    params = CapsuleParameters() # anything passed to init will override values from json/CLI
    logger.setLevel(params.logging_level)

    # if session_id is passed as a command line argument, we will only process that session,
    # otherwise we process all session IDs that match filtering criteria:    
    session_ids = utils.get_df('session')['session_id'].unique().sort()
    logger.info(f"Found {len(session_ids)} session_ids available for use")
    
    if params.session_id is not None and params.session_id in session_ids: 
        logger.info(f"Using single session_id {params.session_id}")
        session_ids = [params.session_id]
    elif params.session_id is not None and params.session_id not in session_ids:
        logger.warning(f"{params.session_id!r} not in filtered session_ids")
    elif utils.is_pipeline(): 
        # only one nwb will be available 
        available_nwb_session_ids = set(p.stem for p in utils.get_nwb_paths())
        session_ids &= available_nwb_session_ids
        assert len(session_ids) <= 1, f"Expected zero or one NWB session_ids in pipeline: got {len(session_ids)}"
    
    # run processing function for each session, with test mode implemented:
    logger.info(f"Launching processing loop with list of {len(session_ids)} session_ids")
    for session_id in session_ids:
        try:
            process_session(params=CapsuleParameters(session_id=session_id))
        except Exception as e:
            logger.exception(f'{session_id} | Failed:')
        else:
            logger.info(f'{session_id} | Completed')

        if params.test:
            logger.info("TEST | Exiting after first session")
            break
    utils.ensure_nonempty_results_dirs('/results') # required for pipeline to work in case this session has no outputs
    logger.info(f"Time elapsed: {time.time() - t0:.2f} s")
    

if __name__ == "__main__":
    main()
