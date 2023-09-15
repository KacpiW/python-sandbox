from datetime import datetime, timezone
from typing import Dict

from end_to_end.config import setup_logger

logger = setup_logger(__name__, "ete.log")


def _convert_datetime_to_utc(time: str) -> datetime:
    """
    Converts a timestamp string into a datetime object in UTC timezone.
    """
    try:
        return datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(timezone.utc)
    except Exception as e:
        logger.error(f"Failed to convert date: {e}")
        raise


def process_vehicles(record: Dict[str, str]):
    """
    Process a vehicle record and extract relevant fields.
    """
    logger.debug("Processing vehicle record.")
    vehicle_records = record["data"]
    return (
        vehicle_records["id"],
        vehicle_records["location"]["lat"],
        vehicle_records["location"]["lng"],
        _convert_datetime_to_utc(vehicle_records["location"]["at"]),
    )


def process_operating_periods(record: Dict[str, str]):
    """
    Process an operating period record and extract relevant fields.
    """
    logger.debug("Processing operating period record.")
    operation_period_records = record["data"]
    return (
        operation_period_records["id"],
        _convert_datetime_to_utc(operation_period_records["start"]),
        _convert_datetime_to_utc(operation_period_records["finish"]),
    )


def process_events(record: Dict[str, str]):
    """
    Process an event record and extract relevant fields.
    """
    logger.debug("Processing event record.")
    entity_type = record["on"]
    data_id = record["data"]["id"]

    return (
        record["event"],
        entity_type,
        _convert_datetime_to_utc(record["at"]),
        record["organization_id"],
        data_id if entity_type == "vehicle" else None,
        data_id if entity_type == "operating_period" else None,
    )
