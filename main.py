# Standard library imports
from io import BytesIO
import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import zipfile
import base64
import importlib.util

# Third-party imports
from bs4 import BeautifulSoup
from syftbox.lib import Client, SyftPermission
import diffprivlib.tools as dp
import numpy as np
import pandas as pd
import tenseal as ts
import yaml

# Local imports
from config import *

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

    if DEVELOPMENT:
        return True

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
    """
    Validate essential configuration parameters.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ValueError: If required keys are missing or epsilon is invalid
    """
    required_keys = ['filepath', 'parameters']
    required_params = ['type', 'epsilon', 'bounds']

    # Check required top-level keys
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing required config keys: {missing_keys}")

    # Check required parameters
    missing_params = [
        param for param in required_params if param not in config['parameters']]
    if missing_params:
        raise ValueError(f"Missing required parameters: {missing_params}")

    # Check epsilon value
    if config['parameters']['epsilon'] <= 0:
        raise ValueError("Epsilon must be positive")


def check_config():
    """
    Check if config.py exists and validate its contents.
    """
    try:
        if not Path('config.py').exists():
            logger.error(
                "config.py not found. Please create config.py from template!")
            return False

        # Import config.py
        spec = importlib.util.spec_from_file_location("config", "config.py")
        if spec is None or spec.loader is None:
            logger.error("Failed to load config.py")
            return False

        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)

        # Convert module attributes to dictionary for validation
        config = {
            'filepath': getattr(config_module, 'FILEPATH', None),
            'parameters': getattr(config_module, 'PARAMETERS', {})
        }

        # Validate configuration
        validate_config(config)
        logger.info("Configuration validated successfully!")
        return True

    except ValueError as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        return False
    except Exception as e:
        logger.error(
            f"Unexpected error while validating configuration: {str(e)}")
        return False


# Following code is from https://github.com/OpenMined/cpu_tracker_member/blob/main/main.py
def create_restricted_public_folder(filepath: Path, client: Client) -> None:
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


def create_private_folder(filepath: Path, client: Client) -> Path:
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


def read_apple_health(filepath, type_parameter=None):
    logger.info("Loading health records ...")

    if filepath.endswith('.zip'):
        logger.info("Unzipping the file")
        with open(filepath, 'rb') as f:
            data = f.read()

        with zipfile.ZipFile(BytesIO(data)) as zip_ref:
            with zip_ref.open('apple_health_export/export.xml') as f:
                file_content = f.read()
    else:
        with open(filepath, 'r') as f:
            file_content = f.read()

    if not file_content:
        logger.error("No export file found named")

    soup = BeautifulSoup(file_content, features='xml')
    records = soup.find_all("Record")

    data_list = []
    if type_parameter:
        for record in records:
            if record.get("type") == type_parameter:
                data_list.append(convert_record_to_dict(record))

    else:
        for record in records:
            data_list.append(convert_record_to_dict(record))

    logger.info("Corresponding health records loaded and parsed")
    # Create the initial dataframe from the XML file, and perform cleansing / preparation

    return pd.DataFrame(data_list)


def clean_up_df(df):
    logger.info("Some data cleanup...")

    # Columns that should be converted to float
    float_columns = ['value']
    # Columns that should be converted to datetime
    datetime_columns = ['creation_date', 'start_date', 'end_date']

    df = df.copy()

    df[float_columns] = df[float_columns].apply(
        pd.to_numeric, errors='coerce')
    df[datetime_columns] = df[datetime_columns].apply(
        pd.to_datetime, errors='coerce')

    # Use end date as the comparison date
    df['date'] = df['end_date'].dt.strftime("%Y-%m-%d")

    # Let's filter some date
    return df[df['date'] >= '2022-01-01']


def create_dp(df, epsilon, bounds_config):

    logger.info("Generating differentially private health records ...")

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
                bounds=bounds,
                dtype=int
            ),
            'dp_step_entries': dp.count_nonzero(
                record_values,
                epsilon=epsilon,
            )
        })

    return pd.DataFrame(dp_df)


def setup_datasites():
    # Following code is from https://github.com/OpenMined/cpu_tracker_member/blob/main/main.py
    client = Client.load()

    # Create an output file with proper read permissions
    restricted_public_folder = client.api_data("health_steps_counter")
    create_restricted_public_folder(restricted_public_folder, client)

    # Create private private folder
    private_folder = create_private_folder(client.datasite_path, client)

    public_file: Path = restricted_public_folder / "health_steps_counter.json"
    private_file: Path = private_folder / "health_steps_counter.json"

    return public_file, private_file


if __name__ == '__main__':

    logger = logging.getLogger(__name__)
    logger.info("Started health steps counter")

    if check_config() and should_run(FILEPATH):

        type_parameter = PARAMETERS['type']
        epsilon = PARAMETERS['epsilon']
        bounds_config = PARAMETERS['bounds']

        df = read_apple_health(FILEPATH, type_parameter)
        df = clean_up_df(df)

        dp_df = create_dp(df, epsilon, bounds_config)

        summary_df = df.groupby('date')['value'].agg(
            ['sum', 'count']).reset_index()
        summary_df.columns = ['date', 'step_count', 'step_entries']

        public_file, private_file = setup_datasites()

        if DEVELOPMENT:
            summary_df.set_index("date").T.to_json("daily_steps.json")
            dp_df.set_index("date").T.to_json("dp_daily_steps.json")
        else:
            summary_df.set_index("date").T.to_json(private_file)
            dp_df.set_index("date").T.to_json(public_file)

        logger.info("Exported the results")

        record_filehash(FILEPATH)
        logger.info("Updated record logs")

    else:
        exit(0)
