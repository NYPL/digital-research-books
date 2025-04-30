import os
from typing import Optional
import yaml
from pathlib import Path
from services.ssm_service import SSMService

LOCAL_SECRETS_FILE = 'config/local-secrets.yaml'

ENV_VAR_TO_SSM_NAME = {
    'CONTENT_CAFE_USER': 'contentcafe/user',
    'CONTENT_CAFE_PSWD': 'contentcafe/pswd',
    'ELASTICSEARCH_PSWD': 'elasticsearch/pswd',
    'ELASTICSEARCH_USER': 'elasticsearch/user',
    'GITHUB_API_KEY': 'github-key',
    'GOOGLE_BOOKS_KEY': 'google-books/api-key',
    'HATHI_API_KEY': 'hathitrust/api-key',
    'HATHI_API_SECRET': 'hathitrust/api-secret',
    'NEW_RELIC_LICENSE_KEY': 'newrelic/key',
    'NYPL_API_CLIENT_ID': 'nypl-api/client-id',
    'NYPL_API_CLIENT_PUBLIC_KEY': 'nypl-api/public-key',
    'NYPL_API_CLIENT_SECRET': 'nypl-api/client-secret',
    'NYPL_BIB_PSWD': 'postgres/nypl-pswd',
    'NYPL_BIB_USER': 'postgres/nypl-user',
    'OCLC_METADATA_ID': 'oclc-metadata-clientid',
    'OCLC_METADATA_SECRET': 'oclc-metadata-secret',
    'OCLC_CLIENT_ID': 'oclc-search-clientid',
    'OCLC_CLIENT_SECRET': 'oclc-search-secret',
    'POSTGRES_PSWD': 'postgres/pswd',
    'POSTGRES_USER': 'postgres/user',
}


def load_env_file(run_type: str, file_string: Optional[str]=None) -> None:
    """Loads configuration details from a specific yaml file.
    Arguments:
        runType {string} -- The environment to load configuration details for.
        fileString {string} -- The file string format indicating where to load
        the configuration file from.
    Raises:
        YAMLError: Indicates malformed yaml markup in the configuration file
    Returns:
        dict -- A dictionary containing the configuration details parsed from
        the specified yaml file.
    """
    env_dict = None

    if file_string:
        open_file = file_string.format(run_type)
    else:
        open_file = 'local.yaml'

    try:
        with open(open_file) as env_stream:
            try:
                env_dict = yaml.full_load(env_stream)
            except yaml.YAMLError as err:
                print(f'{open_file} Invalid! Please review')
                raise err

    except FileNotFoundError as err:
        print('Missing config YAML file! Check directory')
        raise err

    if env_dict:
        for key, value in env_dict.items():
            os.environ[key] = value

    load_secrets()


def _set_env_vars(config: dict) -> None:
    for key, value in config.items():
        if key not in os.environ:
            os.environ[key] = str(value)


def _load_yaml_config(file_path: str) -> None:
    try:
        with open(file_path, "r") as file:
            return yaml.safe_load(file) or {}
    except:
        return {}


def load_secrets():
    if Path(LOCAL_SECRETS_FILE).exists():
        secrets_config = _load_yaml_config(LOCAL_SECRETS_FILE)
        _set_env_vars(secrets_config)

    ssm_service = SSMService()

    for env_var, param_name in ENV_VAR_TO_SSM_NAME.items():
        if os.environ.get(env_var, None) is None:
            param = ssm_service.get_parameter(param_name)

            if param is not None:
                os.environ[env_var] = param
