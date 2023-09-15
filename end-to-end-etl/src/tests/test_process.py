import pytest
from datetime import datetime, timezone
from end_to_end.process import (
    process_vehicles,
    process_operating_periods,
    process_events,
    _convert_datetime_to_utc,
)


@pytest.fixture
def records():
    return [
        {
            "event": "create",
            "on": "operating_period",
            "at": "2023-06-01T08:00:00.000Z",
            "data": {
                "id": "op_123",
                "start": "2023-06-01T08:15:00.000Z",
                "finish": "2023-06-01T08:30:00.000Z",
            },
            "organization_id": "org-1",
        },
        {
            "event": "register",
            "on": "vehicle",
            "at": "2023-06-01T08:10:00.000Z",
            "data": {"id": "veh_123"},
            "organization_id": "org-1",
        },
        {
            "event": "update",
            "on": "vehicle",
            "at": "2023-06-01T08:20:00.000Z",
            "data": {
                "id": "veh_456",
                "location": {"lat": 52, "lng": 13, "at": "2023-06-01T08:20:00.000Z"},
            },
            "organization_id": "org-1",
        },
    ]


def test_process_vehicles(records):
    for record in records:
        if record["on"] == "vehicle" and record["event"] != "register":
            result = process_vehicles(record)
            assert result == (
                "veh_456",
                52,
                13,
                datetime(2023, 6, 1, 6, 20, tzinfo=timezone.utc),
            )


def test_process_operating_periods(records):
    for record in records:
        if record["on"] == "operating_period":
            result = process_operating_periods(record)
            assert result == (
                "op_123",
                "2023-06-01T08:15:00.000Z",
                "2023-06-01T08:30:00.000Z",
            )


def test_process_events(records):
    for record in records:
        result = process_events(record)
        assert result == (
            record["event"],
            record["on"],
            _convert_datetime_to_utc(record["at"]),
            record["organization_id"],
            record["data"]["id"] if record["on"] == "vehicle" else None,
            record["data"]["id"] if record["on"] == "operating_period" else None,
        )
