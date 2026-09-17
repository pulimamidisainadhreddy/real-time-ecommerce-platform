"""
Central configuration for the Spark streaming job.

All environment variables are loaded from the project's .env file.
The configuration is safe to use both locally and from Airflow.
"""

import os
from pathlib import Path

from dotenv import load_dotenv


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


# ============================================================
# Helper functions
# ============================================================

def get_required_env(name: str) -> str:
    """Read a required environment variable."""

    value = os.getenv(name, "").strip()

    if not value:
        raise ValueError(
            f"Missing required environment variable: {name}"
        )

    return value


def get_int_env(name: str, default: int) -> int:
    """Read an integer environment variable."""

    value = os.getenv(
        name,
        str(default),
    ).strip()

    try:
        return int(value)
    except ValueError as error:
        raise ValueError(
            f"{name} must be an integer."
        ) from error


def get_bool_env(name: str, default: bool) -> bool:
    """Read a Boolean environment variable."""

    value = os.getenv(
        name,
        str(default),
    ).strip().lower()

    if value in {"true", "1", "yes"}:
        return True

    if value in {"false", "0", "no"}:
        return False

    raise ValueError(
        f"{name} must be true or false."
    )


def get_path_env(
    name: str,
    default: str,
    container_default: str | None = None,
) -> Path:
    """
    Read a path from the environment.

    Relative paths are resolved safely:
    - inside Airflow, log files use /opt/airflow/logs
    - otherwise they use the project root
    """

    raw_value = os.getenv(name, default).strip()

    if not raw_value:
        raw_value = default

    path = Path(raw_value)

    if path.is_absolute():
        return path

    # Airflow mounts this directory as writable.
    if os.getenv("AIRFLOW_HOME") or Path("/opt/airflow").exists():
        if container_default:
            return Path(container_default)

    return PROJECT_ROOT / path


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = get_required_env(
    "KAFKA_BOOTSTRAP_SERVERS"
)

KAFKA_TOPIC = get_required_env(
    "KAFKA_TOPIC"
)

KAFKA_DLQ_TOPIC = get_required_env(
    "KAFKA_DLQ_TOPIC"
)

KAFKA_STARTING_OFFSETS = os.getenv(
    "KAFKA_STARTING_OFFSETS",
    "earliest",
).strip().lower()

KAFKA_FAIL_ON_DATA_LOSS = get_bool_env(
    "KAFKA_FAIL_ON_DATA_LOSS",
    True,
)

if KAFKA_STARTING_OFFSETS not in {
    "earliest",
    "latest",
}:
    raise ValueError(
        "KAFKA_STARTING_OFFSETS must be "
        "'earliest' or 'latest'."
    )


# ============================================================
# Spark configuration
# ============================================================

SPARK_APP_NAME = os.getenv(
    "SPARK_APP_NAME",
    "OnlineRetailStreaming",
).strip()

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "local[*]",
).strip()

SPARK_MAX_OFFSETS_PER_TRIGGER = get_int_env(
    "SPARK_MAX_OFFSETS_PER_TRIGGER",
    50000,
)

SPARK_TIMEZONE = os.getenv(
    "SPARK_TIMEZONE",
    "UTC",
).strip()

SPARK_LOG_LEVEL = os.getenv(
    "SPARK_LOG_LEVEL",
    "WARN",
).strip().upper()

SPARK_TRIGGER_MODE = os.getenv(
    "SPARK_TRIGGER_MODE",
    "availableNow",
).strip()

if SPARK_MAX_OFFSETS_PER_TRIGGER <= 0:
    raise ValueError(
        "SPARK_MAX_OFFSETS_PER_TRIGGER must be "
        "greater than zero."
    )

if SPARK_TRIGGER_MODE not in {
    "availableNow",
    "processingTime",
}:
    raise ValueError(
        "SPARK_TRIGGER_MODE must be "
        "'availableNow' or 'processingTime'."
    )


# ============================================================
# Checkpoint configuration
# ============================================================

CHECKPOINT_LOCATION = get_required_env(
    "CHECKPOINT_LOCATION"
)

MALFORMED_CHECKPOINT_LOCATION = os.getenv(
    "MALFORMED_CHECKPOINT_LOCATION",
    f"{CHECKPOINT_LOCATION}_malformed",
).strip()


# ============================================================
# Malformed-message configuration
# ============================================================

MALFORMED_MESSAGES_FILE = get_path_env(
    name="MALFORMED_MESSAGES_FILE",
    default="logs/malformed_kafka_messages.jsonl",
    container_default=(
        "/opt/airflow/logs/"
        "malformed_kafka_messages.jsonl"
    ),
)

# Only create the directory when it is writable/needed.
MALFORMED_MESSAGES_FILE.parent.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================
# PostgreSQL configuration
# ============================================================

POSTGRES_HOST = get_required_env(
    "POSTGRES_HOST"
)

POSTGRES_PORT = get_required_env(
    "POSTGRES_PORT"
)

POSTGRES_DB = get_required_env(
    "POSTGRES_DB"
)

POSTGRES_USER = get_required_env(
    "POSTGRES_USER"
)

POSTGRES_PASSWORD = get_required_env(
    "POSTGRES_PASSWORD"
)

POSTGRES_URL = (
    "jdbc:postgresql://"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DB}"
)

POSTGRES_DRIVER = os.getenv(
    "POSTGRES_DRIVER",
    "org.postgresql.Driver",
).strip()

POSTGRES_BATCH_SIZE = get_int_env(
    "POSTGRES_BATCH_SIZE",
    50000,
)

STAGING_TABLE = os.getenv(
    "STAGING_TABLE",
    "public.online_retail_staging",
).strip()

FINAL_TABLE = os.getenv(
    "FINAL_TABLE",
    "public.online_retail",
).strip()

QUALITY_RESULTS_TABLE = os.getenv(
    "QUALITY_RESULTS_TABLE",
    "public.data_quality_results",
).strip()

REJECTED_RECORDS_TABLE = os.getenv(
    "REJECTED_RECORDS_TABLE",
    "public.rejected_online_retail_records",
).strip()

if POSTGRES_BATCH_SIZE <= 0:
    raise ValueError(
        "POSTGRES_BATCH_SIZE must be "
        "greater than zero."
    )


# ============================================================
# Configuration summary
# ============================================================

def get_config_summary() -> dict:
    """Return safe configuration information."""

    return {
        "project_root": str(PROJECT_ROOT),
        "spark_app_name": SPARK_APP_NAME,
        "spark_master": SPARK_MASTER,
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_dlq_topic": KAFKA_DLQ_TOPIC,
        "kafka_starting_offsets": KAFKA_STARTING_OFFSETS,
        "kafka_fail_on_data_loss": KAFKA_FAIL_ON_DATA_LOSS,
        "spark_max_offsets_per_trigger": (
            SPARK_MAX_OFFSETS_PER_TRIGGER
        ),
        "spark_timezone": SPARK_TIMEZONE,
        "spark_log_level": SPARK_LOG_LEVEL,
        "spark_trigger_mode": SPARK_TRIGGER_MODE,
        "checkpoint_location": CHECKPOINT_LOCATION,
        "malformed_checkpoint_location": (
            MALFORMED_CHECKPOINT_LOCATION
        ),
        "malformed_messages_file": (
            str(MALFORMED_MESSAGES_FILE)
        ),
        "postgres_host": POSTGRES_HOST,
        "postgres_port": POSTGRES_PORT,
        "postgres_database": POSTGRES_DB,
        "postgres_driver": POSTGRES_DRIVER,
        "postgres_batch_size": POSTGRES_BATCH_SIZE,
        "staging_table": STAGING_TABLE,
        "final_table": FINAL_TABLE,
        "quality_results_table": QUALITY_RESULTS_TABLE,
        "rejected_records_table": REJECTED_RECORDS_TABLE,
    }
