"""Helper dict to retrieve OS env variables. This list includes all environment variables needed."""

import logging
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# defaults to unit test values.
env_vars = {
    "ENV": "LOCAL",
    "SECRET_KEY": "",
    "ALGORITHM": "HS256",
    "ACCESS_TOKEN_EXPIRE_MINUTES": "120",
    # The Issuers env var will be stored as an array of emails.
    "API_KEY_ISSUERS": [],
    "INITIAL_API_KEY": "",
    "INITIAL_API_KEY_ID": "",
    "CATALOG_NAME": "",
    "SQL_WAREHOUSE_ID": "",
}

# The INSTANCE_HOST is the private IP of CLoudSQL instance e.g. '127.0.0.1' ('172.17.0.1' if deployed to GAE Flex)
engine_vars = {
    "INSTANCE_HOST": "",
    "DB_USER": "",
    "DB_PASS": "",
    "DB_NAME": "",
    "DB_PORT": "",
}

gcs_vars = {
    "GCP_REGION": "",
    "GCP_SERVICE_ACCOUNT_EMAIL": "",
}

# Databricks catalogs that contain institution silver and gold volumes.
ENV_TO_VOLUME_SCHEMA = {"DEV": "dev_sst_02", "STAGING": "staging_sst_01"}

# databricks vars needed for databricks integration
databricks_vars = {
    # SECRET.
    "CATALOG_NAME": "",
    "DATABRICKS_WORKSPACE": "",
    "DATABRICKS_HOST_URL": "",
    # The service account that is used in Databricks to access GCP buckets.
    "DATABRICKS_SERVICE_ACCOUNT_EMAIL": "",
    "GCP_CACHE_BUCKET": "",
}


def startup_env_vars():
    """Setup function to get environment variables. Should be called at startup time."""
    env_file = os.environ.get("ENV_FILE_PATH")
    if not env_file:
        raise ValueError(
            "Missing .env filepath variable. Required. Set ENV_FILE_PATH to full path of .env file."
        )
    load_dotenv(env_file)
    global env_vars
    for name in env_vars:
        env_var = os.environ.get(name)
        if name == "API_KEY_ISSUERS":
            # This is okay to be empty, though slightly unexpected, it shouldn't fail.
            if not env_var:
                continue
            emails = env_var.split(",")
            env_vars[name] = [x.strip() for x in emails]
        if not env_var:
            raise ValueError(
                "Missing " + name + " value missing. Required environment variable."
            )
        if name == "ENV" and env_var not in [
            "PROD",
            "STAGING",
            "DEV",
            "LOCAL",
        ]:
            raise ValueError(
                "ENV environment variable not one of: PROD, STAGING, DEV, LOCAL."
            )
        if (
            name in ("ACCESS_TOKEN_EXPIRE_MINUTES", "ACCESS_TOKEN_EXPIRE_MINUTES")
        ) and not env_var.isdigit():
            raise ValueError(
                "ACCESS_TOKEN_EXPIRE_MINUTES and ACCESS_TOKEN_EXPIRE_MINUTES environment variables must be an int."
            )
        env_vars[name] = env_var
    if env_vars["ENV"] != "LOCAL":
        global gcs_vars
        for name in gcs_vars:
            env_var = os.environ.get(name)
            if not env_var:
                raise ValueError(
                    "Missing "
                    + name
                    + " value missing. Required GCP environment variable."
                )
            gcs_vars[name] = env_var
        global databricks_vars
        for name in databricks_vars:
            env_var = os.environ.get(name)
            if not env_var or env_var == "":
                raise ValueError(
                    "Missing "
                    + name
                    + " value missing. Required Databricks integration environment variable."
                )
            databricks_vars[name] = env_var


def db_connection() -> str:
    value = (os.environ.get("DB_CONNECTION") or "").strip().lower()
    if not value:
        return (
            "sqlite" if os.environ.get("ENV", "LOCAL").upper() == "LOCAL" else "mysql"
        )
    if value not in ("sqlite", "mysql"):
        raise ValueError("DB_CONNECTION must be sqlite or mysql.")
    return value


def ssl_connect_args() -> dict:
    args = {}
    missing = []
    for env_name, arg_name in (
        ("DB_ROOT_CERT", "ssl_ca"),
        ("DB_CERT", "ssl_cert"),
        ("DB_KEY", "ssl_key"),
    ):
        value = os.environ.get(env_name)
        if value:
            args[arg_name] = value
        else:
            missing.append(env_name)
    if missing and os.environ.get("ENV", "LOCAL").upper() != "LOCAL":
        logger.warning(
            "Connecting without SSL client certs; unset: %s", ", ".join(missing)
        )
    return args


def setup_database_vars():
    """Setup function to get db environment variables. Should be called at db startup time."""
    global engine_vars
    for name in engine_vars:
        env_var = os.environ.get(name)
        if name == "DB_PASS":
            engine_vars[name] = env_var or ""
            continue
        if not env_var:
            raise ValueError("Missing " + name + " value missing. Required.")
        engine_vars[name] = env_var
