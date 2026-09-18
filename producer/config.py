import os
from pathlib import Path

from dotenv import load_dotenv


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)


# ============================================================
# Helper functions
# ============================================================

def get_string_env(name: str, default: str = "") -> str:
    """Read a string environment variable."""

    return os.getenv(name, default).strip()


def get_int_env(name: str, default: int) -> int:
    """Read an integer environment variable."""

    value = os.getenv(name, str(default)).strip()

    try:
        return int(value)
    except ValueError as error:
        raise ValueError(
            f"{name} must be an integer. Received: {value}"
        ) from error


def get_float_env(name: str, default: float) -> float:
    """Read a floating-point environment variable."""

    value = os.getenv(name, str(default)).strip()

    try:
        return float(value)
    except ValueError as error:
        raise ValueError(
            f"{name} must be a number. Received: {value}"
        ) from error


def get_bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable."""

    value = os.getenv(
        name,
        str(default),
    ).strip().lower()

    if value in {"true", "1", "yes", "y", "on"}:
        return True

    if value in {"false", "0", "no", "n", "off"}:
        return False

    raise ValueError(
        f"{name} must be a boolean value."
    )


def get_path_env(name: str, default: Path) -> Path:
    """Read and normalize a project-relative path."""

    value = os.getenv(
        name,
        str(default),
    ).strip()

    path = Path(value)

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path.resolve()


# ============================================================
# Validation helpers
# ============================================================

def validate_positive_integer(name: str, value: int) -> None:
    """Validate a positive integer."""

    if value <= 0:
        raise ValueError(
            f"{name} must be greater than zero."
        )


def validate_non_negative_integer(
    name: str,
    value: int,
) -> None:
    """Validate a non-negative integer."""

    if value < 0:
        raise ValueError(
            f"{name} cannot be negative."
        )


def validate_non_negative_number(
    name: str,
    value: float,
) -> None:
    """Validate a non-negative number."""

    if value < 0:
        raise ValueError(
            f"{name} cannot be negative."
        )


def validate_topic_name(name: str, value: str) -> None:
    """Validate a Kafka topic name."""

    if not value:
        raise ValueError(
            f"{name} cannot be empty."
        )

    if len(value) > 249:
        raise ValueError(
            f"{name} is too long."
        )

    invalid_characters = {
        " ",
        ",",
        ":",
        '"',
        "'",
        ";",
        "/",
        "\\",
    }

    if any(
        character in value
        for character in invalid_characters
    ):
        raise ValueError(
            f"{name} contains invalid Kafka topic characters."
        )


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = get_string_env(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = get_string_env(
    "KAFKA_TOPIC",
    "online_retail",
)

KAFKA_DLQ_TOPIC = get_string_env(
    "KAFKA_DLQ_TOPIC",
    f"{KAFKA_TOPIC}_dlq",
)


# ============================================================
# Kafka producer settings
# ============================================================

KAFKA_BATCH_SIZE = get_int_env(
    "KAFKA_BATCH_SIZE",
    32768,
)

KAFKA_LINGER_MS = get_int_env(
    "KAFKA_LINGER_MS",
    10,
)

KAFKA_RETRIES = get_int_env(
    "KAFKA_RETRIES",
    3,
)

KAFKA_REQUEST_TIMEOUT_MS = get_int_env(
    "KAFKA_REQUEST_TIMEOUT_MS",
    30000,
)


# ============================================================
# Application producer settings
# ============================================================

PRODUCER_BATCH_SIZE = get_int_env(
    "PRODUCER_BATCH_SIZE",
    10000,
)

PRODUCER_SEND_DELAY = get_float_env(
    "PRODUCER_SEND_DELAY",
    0.0,
)

PRODUCER_REPLAY = get_bool_env(
    "PRODUCER_REPLAY",
    False,
)


# ============================================================
# Failed-message handling
# ============================================================

FAILED_MESSAGES_FILE = get_path_env(
    "FAILED_MESSAGES_FILE",
    PROJECT_ROOT / "logs" / "failed_messages.jsonl",
)

MAX_SEND_ATTEMPTS = get_int_env(
    "MAX_SEND_ATTEMPTS",
    3,
)

RETRY_DELAY = get_float_env(
    "RETRY_DELAY",
    2.0,
)


# ============================================================
# Checkpoint configuration
# ============================================================

CHECKPOINT_FILE = get_path_env(
    "PRODUCER_CHECKPOINT_FILE",
    PROJECT_ROOT / ".producer_checkpoint",
)


# ============================================================
# Data configuration
# ============================================================

DATA_FILE = get_path_env(
    "DATA_FILE",
    PROJECT_ROOT
    / "data"
    / "raw"
    / "Online Retail.xlsx",
)


# ============================================================
# Configuration validation
# ============================================================

if not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError(
        "KAFKA_BOOTSTRAP_SERVERS cannot be empty."
    )

validate_topic_name(
    "KAFKA_TOPIC",
    KAFKA_TOPIC,
)

validate_topic_name(
    "KAFKA_DLQ_TOPIC",
    KAFKA_DLQ_TOPIC,
)

validate_positive_integer(
    "KAFKA_BATCH_SIZE",
    KAFKA_BATCH_SIZE,
)

validate_non_negative_integer(
    "KAFKA_LINGER_MS",
    KAFKA_LINGER_MS,
)

validate_non_negative_integer(
    "KAFKA_RETRIES",
    KAFKA_RETRIES,
)

validate_positive_integer(
    "KAFKA_REQUEST_TIMEOUT_MS",
    KAFKA_REQUEST_TIMEOUT_MS,
)

validate_positive_integer(
    "PRODUCER_BATCH_SIZE",
    PRODUCER_BATCH_SIZE,
)

validate_non_negative_number(
    "PRODUCER_SEND_DELAY",
    PRODUCER_SEND_DELAY,
)

validate_positive_integer(
    "MAX_SEND_ATTEMPTS",
    MAX_SEND_ATTEMPTS,
)

validate_non_negative_number(
    "RETRY_DELAY",
    RETRY_DELAY,
)