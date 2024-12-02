# Imports
import yaml
import logging
import os
import hashlib
import datetime
import json

import pandas as pd
import numpy as np
import diffprivlib.tools as dp

from bs4 import BeautifulSoup
from pathlib import Path
from syftbox.lib import Client, SyftPermission


# Defining logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def calculate_file_hash(filepath: str) -> str:
    """
    Calculate SHA-256 hash of a file

    Args:
        filepath (str): Path to the file to hash

    Returns:
        str: Hexadecimal string of the hash
    """
    sha256_hash = hashlib.sha256()

    with open(filepath, 'rb') as f:
        # Read the file in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(4096), b''):
            sha256_hash.update(chunk)

    return sha256_hash.hexdigest()


def should_run(filepath: str) -> bool:
    '''
    Check whether current file on filepath hash is the same with the one last recorded

    Args:
        filepath (str): Path to the file to check

    Returns:
        bool: True if file has changed or no previous hash exists, False otherwise
    '''
    hashes_file = f"./hashes/{API_NAME}_last_run"

    # Calculate current file hash
    current_hash = calculate_file_hash(filepath)

    # If hashes directory or file doesn't exist, we should run
    if not os.path.exists(hashes_file):
        return True

    try:
        with open(hashes_file, 'r') as f:
            stored_hash = json.load(f).get('hash')

        # Return True if hashes are different (file has changed)
        return current_hash != stored_hash

    except (json.JSONDecodeError, KeyError):
        # If there's any error reading the hash, we should run to be safe
        return True


def record_filehash(filepath: str) -> None:
    '''
    Store the current filepath hash 

    Args:
        filepath (str): Path to the file whose hash should be stored
    '''
    hashes_file = f"./hashes/{API_NAME}_last_run"
    current_hash = calculate_file_hash(filepath)

    # Create hashes directory if it doesn't exist
    os.makedirs(os.path.dirname(hashes_file), exist_ok=True)

    # Store hash in JSON format with timestamp for debugging purposes
    hash_data = {
        'hash': current_hash,
        'timestamp': datetime.datetime.now().isoformat()
    }

    with open(hashes_file, 'w') as f:
        json.dump(hash_data, f, indent=2)


def validate_config(config):
    required_keys = ['filepath', 'parameters']
    required_params = ['type', 'epsilon', 'bounds']

    if not all(key in config for key in required_keys):
        raise ValueError(f"Missing required config keys: {required_keys}")
    if not all(param in config['parameters'] for param in required_params):
        raise ValueError(f"Missing required parameters: {required_params}")
    if config['parameters']['epsilon'] <= 0:
        raise ValueError("Epsilon must be positive")


# Following code is from https://github.com/OpenMined/cpu_tracker_member/blob/main/main.py
def create_restricted_public_folder(filepath: Path) -> None:
    """
    Create an output folder for Health Steps data within the specified path.

    This function creates a directory structure for storing Health Steps data under `api_data`. If the directory
    already exists, it will not be recreated. Additionally, default permissions for accessing the created folder are set using the
    `SyftPermission` mechanism to allow the data to be read by an aggregator.

    Args:
        path (Path): The base path where the output folder should be created.

    """
    os.makedirs(filepath, exist_ok=True)

    # Set default permissions for the created folder
    permissions = SyftPermission.datasite_default(email=client.email)
    permissions.read.append(AGGREGATOR_DATASITE)
    permissions.save(filepath)


def create_private_folder(filepath: Path) -> Path:
    """
    Create a private folder for Health Steps data within the specified path.

    This function creates a directory structure for storing Health Steps data under `private/filepath`.
    If the directory already exists, it will not be recreated. Additionally, default permissions for
    accessing the created folder are set using the `SyftPermission` mechanism, allowing the data to be
    accessible only by the owner's email.

    Args:
        path (Path): The base path where the output folder should be created.

    Returns:
        Path: The path to the created directory.
    """
    path: Path = filepath / "private" / "health_steps_counter"
    os.makedirs(path, exist_ok=True)

    # Set default permissions for the created folder
    permissions = SyftPermission.datasite_default(email=client.email)
    permissions.save(path)

    return path


# Following code is from https://github.com/OpenMined/cpu_tracker_member/blob/main/main.py
def convert_record_to_dict(record):
    data = {
        'type': record.get('type'),
        'source_name': record.get('sourceName'),
        'source_version': record.get('sourceVersion'),
        'unit': record.get('unit'),
        'value': record.get('value'),
        'creation_date': record.get('creationDate'),
        'start_date': record.get('startDate'),
        'end_date': record.get('endDate')
    }

    return data


if __name__ == '__main__':

    logger = logging.getLogger(__name__)
    logger.info("Started health steps counter")

    try:
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        validate_config(config)
    except ValueError as e:
        logger.error(str(e))
        exit()
    except FileNotFoundError as e:
        logger.error("config.yaml not found. Please create config.yaml (see readme for details)!")
        exit()

    # Parse Config
    API_NAME = config['api_name']
    AGGREGATOR_DATASITE = config['aggregator_datasite']

    filepath = config['filepath']
    type_parameter = config['parameters']['type']
    epsilon = config['parameters']['epsilon']
    bounds_config = config['parameters']['bounds']

    if should_run(filepath):

        logger.info("Loading health records ...")
        with open(filepath, 'r') as f:
            data = f.read()

        soup = BeautifulSoup(data, features='xml')
        records = soup.find_all("Record")

        data_list = []
        for record in records:
            if record.get("type") == type_parameter:
                data_list.append(convert_record_to_dict(record))

        logger.info("Corresponding health records loaded and parsed")

        # Create the initial dataframe from the XML file, and perform cleansing / preparation
        df = pd.DataFrame(data_list)

        # Columns that should be converted to float
        float_columns = ['value']
        # Columns that should be converted to datetime
        datetime_columns = ['creation_date', 'start_date', 'end_date']

        df[float_columns] = df[float_columns].apply(
            pd.to_numeric, errors='coerce')
        df[datetime_columns] = df[datetime_columns].apply(
            pd.to_datetime, errors='coerce')

        # Use end date as the comparison date
        df['date'] = df['end_date'].dt.strftime("%Y-%m-%d")

        summary_df = df.groupby('date')['value'].agg(
            ['sum', 'count']).reset_index()
        summary_df.columns = ['date', 'step_count', 'step_entries']

        # Create the differentially private dataframe
        dp_df = []

        for date in df['date'].unique():

            record_values = df[df['date'] == date]['value']

            if bounds_config == 'auto-local':
                bounds = (1, record_values.max())

            dp_df.append({
                'date': date,
                'dp_step_count': dp.sum(
                    record_values,
                    epsilon=epsilon,
                    bounds=bounds
                ),
                'dp_step_entries': dp.count_nonzero(
                    record_values,
                    epsilon=epsilon
                )
            })

        dp_df = pd.DataFrame(dp_df)

        # Following code is from https://github.com/OpenMined/cpu_tracker_member/blob/main/main.py
        client = Client.load()

        # Create an output file with proper read permissions
        restricted_public_folder = client.api_data("health_steps_counter")
        create_restricted_public_folder(restricted_public_folder)

        # Create private private folder
        private_folder = create_private_folder(client.datasite_path)

        public_mean_file: Path = restricted_public_folder / "health_steps_counter.json"
        private_mean_file: Path = private_folder / "health_steps_counter.json"

        logger.info(f"Test: {public_mean_file}")
        logger.info(f"Test: {private_mean_file}")

        summary_df.set_index("date").T.to_json(private_mean_file)
        dp_df.set_index("date").T.to_json(public_mean_file)

        # summary_df.set_index("date").T.to_json("daily_steps.json")
        # dp_df.set_index("date").T.to_json("dp_daily_steps.json")

        logger.info("Exported the results")
        
        record_filehash(filepath)
        logger.info("Updated record logs")
        
    else:
        exit(0)
