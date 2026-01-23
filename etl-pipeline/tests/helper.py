import os


class TestHelpers:
    ENV_VARS = {
        "POSTGRES_HOST": "test_psql_host",
        "POSTGRES_PORT": "test_psql_port",
        "POSTGRES_USER": "test_psql_user",
        "POSTGRES_PSWD": "test_psql_pswd",
        "POSTGRES_NAME": "test_psql_name",
        "POSTGRES_ADMIN_USER": "test_psql_admin",
        "POSTGRES_ADMIN_PSWD": "test_psql_admin_pswd",
        "REDIS_HOST": "test_redis_host",
        "REDIS_PORT": "test_redis_port",
        "DRB_ELASTICSEARCH_INDEX": "test_es_index",
        "DRB_ELASTICSEARCH_HOST": "test_es_host",
        "DRB_ELASTICSEARCH_PORT": "test_es_port",
        "DRB_ELASTICSEARCH_TIMEOUT": "test_es_timeout",
        "OCLC_API_KEY": "test_oclc_key",
        "OCLC_CLASSIFY_API_KEY": "test_classify_key",
        "AWS_REGION": "test_aws_region",
        "FILE_BUCKET": "test_aws_bucket",
        "NYPL_BIB_HOST": "test_bib_host",
        "NYPL_BIB_PORT": "test_bib_port",
        "NYPL_BIB_NAME": "test_bib_name",
        "NYPL_BIB_USER": "test_bib_user",
        "NYPL_BIB_PSWD": "test_bib_pswd",
        "NYPL_LOCATIONS_BY_CODE": "test_location_url",
        "NYPL_API_CLIENT_ID": "test_api_client",
        "NYPL_API_CLIENT_SECRET": "test_api_secret",
        "NYPL_API_CLIENT_TOKEN_URL": "test_api_token_url",
        "GITHUB_API_KEY": "test_github_key",
        "BARDO_CCE_API": "test_cce_url",
        "ENVIRONMENT": "test",
    }

    @classmethod
    def setEnvVars(cls):
        for key, value in cls.ENV_VARS.items():
            os.environ[key] = value

    @classmethod
    def clearEnvVars(cls):
        for key in cls.ENV_VARS.keys():
            os.environ[key] = ""
