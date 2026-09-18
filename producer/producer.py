import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

try:
    from .config import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        KAFKA_DLQ_TOPIC,
        KAFKA_BATCH_SIZE,
        KAFKA_LINGER_MS,
        KAFKA_RETRIES,
        KAFKA_REQUEST_TIMEOUT_MS,
        PRODUCER_BATCH_SIZE,
        PRODUCER_SEND_DELAY,
        PRODUCER_REPLAY,
        CHECKPOINT_FILE,
        DATA_FILE,
        FAILED_MESSAGES_FILE,
        MAX_SEND_ATTEMPTS,
        RETRY_DELAY,
    )
except ImportError:
    from config import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        KAFKA_DLQ_TOPIC,
        KAFKA_BATCH_SIZE,
        KAFKA_LINGER_MS,
        KAFKA_RETRIES,
        KAFKA_REQUEST_TIMEOUT_MS,
        PRODUCER_BATCH_SIZE,
        PRODUCER_SEND_DELAY,
        PRODUCER_REPLAY,
        CHECKPOINT_FILE,
        DATA_FILE,
        FAILED_MESSAGES_FILE,
        MAX_SEND_ATTEMPTS,
        RETRY_DELAY,
    )


# ============================================================
# Logging
# ============================================================

class JsonFormatter(logging.Formatter):
    """Format log messages as JSON."""

    def format(self, record):
        log_record = {
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field in [
            "row_index",
            "topic",
            "partition",
            "offset",
            "attempt",
            "error",
        ]:
            if hasattr(record, field):
                log_record[field] = getattr(
                    record,
                    field,
                )

        if record.exc_info:
            log_record["exception"] = self.formatException(
                record.exc_info
            )

        return json.dumps(log_record)


def configure_logging():
    """Configure structured logging."""

    logger_instance = logging.getLogger(
        "retail_producer"
    )

    logger_instance.setLevel(logging.INFO)
    logger_instance.handlers.clear()
    logger_instance.propagate = False

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    logger_instance.addHandler(handler)

    return logger_instance


logger = configure_logging()


# ============================================================
# Kafka producer
# ============================================================

def create_producer():
    """Create an idempotent Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        key_serializer=lambda key: (
            None
            if key is None
            else str(key).encode("utf-8")
        ),

        value_serializer=lambda value: json.dumps(
            value,
            default=str,
        ).encode("utf-8"),

        batch_size=KAFKA_BATCH_SIZE,
        linger_ms=KAFKA_LINGER_MS,
        compression_type="gzip",
        acks="all",
        retries=KAFKA_RETRIES,
        request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
        enable_idempotence=True,
    )


# ============================================================
# Load data
# ============================================================

def load_data():
    """Load the Excel dataset."""

    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Input data file not found: {DATA_FILE}"
        )

    df = pd.read_excel(
        DATA_FILE,
        usecols=[
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ],
    )

    if df.empty:
        raise ValueError(
            "Input dataset contains no records."
        )

    required_columns = {
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if not pd.api.types.is_numeric_dtype(
        df["Quantity"]
    ):
        raise ValueError(
            "Quantity must be numeric."
        )

    if not pd.api.types.is_numeric_dtype(
        df["UnitPrice"]
    ):
        raise ValueError(
            "UnitPrice must be numeric."
        )

    negative_price_count = int(
        (df["UnitPrice"] < 0).sum()
    )

    if negative_price_count > 0:
        logger.warning(
            "Negative UnitPrice values detected. "
            "Those rows will be sent to the DLQ.",
            extra={
                "row_index": negative_price_count,
                "error": "negative UnitPrice values",
            },
        )

    logger.info(
        "Dataset loaded successfully",
        extra={
            "row_index": len(df),
        },
    )

    return df


# ============================================================
# Event creation
# ============================================================

def create_event(row):
    """Convert one dataset row into a Kafka event."""

    return {
        "event_version": "1.0",

        "invoice_no": (
            None
            if pd.isna(row.InvoiceNo)
            else str(row.InvoiceNo)
        ),

        "stock_code": (
            None
            if pd.isna(row.StockCode)
            else str(row.StockCode)
        ),

        "description": (
            None
            if pd.isna(row.Description)
            else str(row.Description)
        ),

        "quantity": (
            None
            if pd.isna(row.Quantity)
            else int(row.Quantity)
        ),

        "invoice_date": (
            None
            if pd.isna(row.InvoiceDate)
            else str(row.InvoiceDate)
        ),

        "unit_price": (
            None
            if pd.isna(row.UnitPrice)
            else float(row.UnitPrice)
        ),

        "customer_id": (
            None
            if pd.isna(row.CustomerID)
            else str(row.CustomerID)
        ),

        "country": (
            None
            if pd.isna(row.Country)
            else str(row.Country)
        ),
    }


# ============================================================
# Kafka message key
# ============================================================

def create_message_key(event):
    """
    Create a stable Kafka key.

    Records with the same invoice number are sent
    to the same Kafka partition.
    """

    invoice_no = event.get("invoice_no")

    if invoice_no:
        return str(invoice_no)

    stock_code = event.get("stock_code")

    if stock_code:
        return str(stock_code)

    return "unknown-record"


# ============================================================
# Row validation
# ============================================================

def validate_row(row):
    """Validate one dataset row."""

    required_fields = {
        "InvoiceNo": row.InvoiceNo,
        "StockCode": row.StockCode,
        "Quantity": row.Quantity,
        "UnitPrice": row.UnitPrice,
        "InvoiceDate": row.InvoiceDate,
    }

    for field_name, value in required_fields.items():
        if pd.isna(value):
            return False, f"Missing {field_name}"

    if not isinstance(
        row.Quantity,
        (int, float),
    ):
        return False, "Quantity must be numeric"

    if not isinstance(
        row.UnitPrice,
        (int, float),
    ):
        return False, "UnitPrice must be numeric"

    if row.UnitPrice < 0:
        return False, "UnitPrice cannot be negative"

    return True, ""


# ============================================================
# Failed message storage
# ============================================================

def save_failed_message(
    row_index,
    event,
    error,
):
    """Save permanently failed messages to a JSONL file."""

    FAILED_MESSAGES_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    failed_record = {
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "row_index": row_index,
        "event": event,
        "error": str(error),
    }

    with FAILED_MESSAGES_FILE.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(
                failed_record,
                default=str,
            ) + "\n"
        )

    logger.error(
        "Failed message saved",
        extra={
            "row_index": row_index,
            "error": str(error),
        },
    )


# ============================================================
# Dead-letter topic
# ============================================================

def create_dlq_event(row, reason):
    """Create a dead-letter event."""

    raw_record = {
        column: (
            None
            if pd.isna(value)
            else str(value)
        )
        for column, value in row._asdict().items()
    }

    return {
        "event_version": "1.0",
        "error_reason": reason,
        "source_topic": KAFKA_TOPIC,
        "failed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "record": raw_record,
    }


def send_to_dlq(
    producer,
    row,
    reason,
    row_index,
):
    """Send an invalid record to the Kafka DLQ topic."""

    dlq_event = create_dlq_event(
        row,
        reason,
    )

    key = (
        dlq_event["record"].get("InvoiceNo")
        or dlq_event["record"].get("StockCode")
        or "invalid-record"
    )

    try:
        future = producer.send(
            KAFKA_DLQ_TOPIC,
            key=key,
            value=dlq_event,
        )

        metadata = future.get(
            timeout=KAFKA_REQUEST_TIMEOUT_MS / 1000
        )

        logger.warning(
            "Invalid record sent to DLQ",
            extra={
                "row_index": row_index,
                "topic": KAFKA_DLQ_TOPIC,
                "partition": metadata.partition,
                "offset": metadata.offset,
            },
        )

        return True

    except Exception as error:
        logger.exception(
            "Could not send invalid record to DLQ",
            extra={
                "row_index": row_index,
                "topic": KAFKA_DLQ_TOPIC,
                "error": str(error),
            },
        )

        save_failed_message(
            row_index=row_index,
            event=dlq_event,
            error=error,
        )

        return False


# ============================================================
# Checkpoint handling
# ============================================================

def load_checkpoint():
    """Load the next row index to process."""

    if PRODUCER_REPLAY:
        logger.warning(
            "Replay mode enabled. Starting from row zero."
        )
        return 0

    if not CHECKPOINT_FILE.exists():
        return 0

    try:
        checkpoint = int(
            CHECKPOINT_FILE.read_text(
                encoding="utf-8"
            ).strip()
        )

        if checkpoint < 0:
            raise ValueError

        return checkpoint

    except ValueError:
        logger.warning(
            "Invalid checkpoint. Starting from row zero."
        )
        return 0


def save_checkpoint(row_number):
    """
    Save checkpoint safely on Windows.

    Retries when the checkpoint file is temporarily locked.
    If Windows keeps the file locked, the producer continues
    instead of crashing.
    """

    CHECKPOINT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_file = CHECKPOINT_FILE.with_name(
        f"{CHECKPOINT_FILE.name}.{__import__('os').getpid()}.tmp"
    )

    try:
        with temporary_file.open(
            "w",
            encoding="utf-8",
        ) as file:
            file.write(str(row_number))
            file.flush()
            __import__("os").fsync(file.fileno())

        last_error = None

        for attempt in range(1, 6):
            try:
                __import__("os").replace(
                    temporary_file,
                    CHECKPOINT_FILE,
                )

                logger.info(
                    "Checkpoint saved",
                    extra={
                        "row_index": row_number,
                    },
                )

                return True

            except PermissionError as error:
                last_error = error

                if attempt < 5:
                    time.sleep(0.5)

        logger.warning(
            "Checkpoint file is locked. "
            "Producer will continue without updating checkpoint.",
            extra={
                "row_index": row_number,
                "error": str(last_error),
            },
        )

        return False

    except OSError as error:
        logger.warning(
            "Could not save checkpoint. "
            "Producer will continue.",
            extra={
                "row_index": row_number,
                "error": str(error),
            },
        )

        return False

    finally:
        try:
            if temporary_file.exists():
                temporary_file.unlink()
        except OSError:
            pass


# ============================================================
# Reliable message delivery
# ============================================================

def send_with_retry(
    producer,
    topic,
    event,
    row_index,
):
    """Send one Kafka event with retries."""

    message_key = create_message_key(event)
    last_error = None

    for attempt in range(
        1,
        MAX_SEND_ATTEMPTS + 1,
    ):
        try:
            future = producer.send(
                topic,
                key=message_key,
                value=event,
            )

            metadata = future.get(
                timeout=KAFKA_REQUEST_TIMEOUT_MS / 1000
            )

            logger.info(
                "Message delivered",
                extra={
                    "row_index": row_index,
                    "topic": topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "attempt": attempt,
                },
            )

            return True

        except Exception as error:
            last_error = error

            logger.warning(
                "Message delivery failed",
                extra={
                    "row_index": row_index,
                    "topic": topic,
                    "attempt": attempt,
                    "error": str(error),
                },
            )

            if attempt < MAX_SEND_ATTEMPTS:
                time.sleep(RETRY_DELAY)

    save_failed_message(
        row_index=row_index,
        event=event,
        error=last_error,
    )

    return False


# ============================================================
# Send records
# ============================================================

def _wait_for_batch(producer, pending_messages):
    """
    Flush a batch and verify Kafka delivery.

    Messages are sent asynchronously. We only wait once per batch,
    instead of waiting for every individual message.
    """

    if not pending_messages:
        return True, None

    producer.flush()

    first_error = None

    for item in pending_messages:
        future = item["future"]

        try:
            future.get(
                timeout=KAFKA_REQUEST_TIMEOUT_MS / 1000
            )

        except Exception as error:
            if first_error is None:
                first_error = {
                    "row_index": item["row_index"],
                    "event": item["event"],
                    "error": error,
                }

    pending_messages.clear()

    if first_error is not None:
        return False, first_error

    return True, None


def send_records(df, producer):
    """
    Send records to Kafka in asynchronous 50,000-record batches.

    The producer no longer waits for Kafka after every record.
    Checkpoints are saved only after a complete batch has been
    successfully delivered.
    """

    start_index = load_checkpoint()

    sent_count = 0
    dlq_count = 0
    failed_count = 0

    batch_size = 50000
    pending_messages = []

    logger.info(
        "Producer started with asynchronous batch size 50000",
        extra={
            "row_index": start_index,
            "topic": KAFKA_TOPIC,
        },
    )

    for index, row in enumerate(
        df.itertuples(index=False),
        start=0,
    ):
        if index < start_index:
            continue

        is_valid, reason = validate_row(row)

        if not is_valid:
            dlq_event = create_dlq_event(row, reason)

            key = (
                dlq_event["record"].get("InvoiceNo")
                or dlq_event["record"].get("StockCode")
                or "invalid-record"
            )

            try:
                future = producer.send(
                    KAFKA_DLQ_TOPIC,
                    key=key,
                    value=dlq_event,
                )

                pending_messages.append(
                    {
                        "future": future,
                        "row_index": index,
                        "event": dlq_event,
                        "is_dlq": True,
                    }
                )

                dlq_count += 1

            except Exception as error:
                save_failed_message(
                    row_index=index,
                    event=dlq_event,
                    error=error,
                )

                failed_count += 1
                break

        else:
            event = create_event(row)

            try:
                future = producer.send(
                    KAFKA_TOPIC,
                    key=create_message_key(event),
                    value=event,
                )

                pending_messages.append(
                    {
                        "future": future,
                        "row_index": index,
                        "event": event,
                        "is_dlq": False,
                    }
                )

                sent_count += 1

            except Exception as error:
                save_failed_message(
                    row_index=index,
                    event=event,
                    error=error,
                )

                failed_count += 1
                break

        records_in_current_batch = (
            index - start_index + 1
        )

        if (
            records_in_current_batch % batch_size == 0
        ):
            batch_start = (
                index - batch_size + 1
            )

            success, error_info = _wait_for_batch(
                producer,
                pending_messages,
            )

            if not success:
                save_failed_message(
                    row_index=error_info["row_index"],
                    event=error_info["event"],
                    error=error_info["error"],
                )

                failed_count += 1

                logger.error(
                    "Kafka batch failed at row %s.",
                    error_info["row_index"],
                )

                break

            save_checkpoint(index + 1)

            logger.info(
                "Producer batch completed: rows %s-%s",
                batch_start,
                index,
                extra={
                    "row_index": index,
                    "topic": KAFKA_TOPIC,
                },
            )

            if PRODUCER_SEND_DELAY > 0:
                time.sleep(
                    PRODUCER_SEND_DELAY
                )

    else:
        # Process the final partial batch.
        if pending_messages:
            success, error_info = _wait_for_batch(
                producer,
                pending_messages,
            )

            if success:
                save_checkpoint(len(df))
            else:
                save_failed_message(
                    row_index=error_info["row_index"],
                    event=error_info["event"],
                    error=error_info["error"],
                )

                failed_count += 1

    # If the loop stopped early, make sure any queued messages are flushed.
    if pending_messages:
        producer.flush()
        pending_messages.clear()

    logger.info(
        "Producer completed",
        extra={
            "row_index": sent_count,
            "error": (
                f"dlq_count={dlq_count}, "
                f"failed_count={failed_count}"
            ),
        },
    )

    return (
        sent_count,
        dlq_count,
        failed_count,
    )


# ============================================================
# Main
# ============================================================

def main():
    """Run the Kafka producer."""

    producer = None

    try:
        logger.info(
            "Starting Kafka producer",
            extra={
                "topic": KAFKA_TOPIC,
            },
        )

        df = load_data()
        producer = create_producer()

        sent_count, dlq_count, failed_count = send_records(
            df,
            producer,
        )

        logger.info(
            "Final producer statistics",
            extra={
                "row_index": sent_count,
                "error": (
                    f"dlq_count={dlq_count}, "
                    f"failed_count={failed_count}"
                ),
            },
        )

    except Exception:
        logger.exception(
            "Producer execution failed"
        )
        raise

    finally:
        if producer is not None:
            producer.close()

        logger.info(
            "Kafka producer stopped"
        )


if __name__ == "__main__":
    main()