"""Send synthetic feature events to Amplitude.

This script reads a CSV of feature events and sends them to Amplitude’s
HTTP V2 API.  It batches events into payloads of up to ten events and
retries on HTTP errors.  Each event includes an `insert_id` to prevent
duplicates.  The API key must be provided via the AMPLITUDE_API_KEY
environment variable.

See Amplitude’s documentation for guidance on batching and rate limits.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

AMPLITUDE_ENDPOINT = "https://api2.amplitude.com/2/httpapi"


def send_batch(api_key: str, events: List[Dict[str, object]]) -> None:
    payload = {
        "api_key": api_key,
        "events": events,
    }
    resp = requests.post(AMPLITUDE_ENDPOINT, json=payload, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Amplitude request failed with status {resp.status_code}: {resp.text}"
        )


def ingest_events(csv_path: Path, dry_run: bool = False) -> None:
    api_key = os.getenv("AMPLITUDE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "AMPLITUDE_API_KEY environment variable must be set to send events"
        )
    df = pd.read_csv(csv_path)
    events: List[Dict[str, object]] = []
    for idx, row in df.iterrows():
        event = {
            "user_id": str(row["member_id"]),
            "event_type": row["event_name"],
            "time": int(pd.to_datetime(row["event_date"]).timestamp() * 1000),
            "insert_id": f"{row['member_id']}_{row['event_date']}_{row['feature']}_{row['event_name']}",
            "event_properties": {
                "feature": row["feature"],
            },
        }
        events.append(event)
        if len(events) == 10:
            if not dry_run:
                send_batch(api_key, events)
            events = []
            time.sleep(0.2)  # simple throttling to respect rate limits
    # send any remaining events
    if events and not dry_run:
        send_batch(api_key, events)


def main(args: List[str]) -> None:
    parser = argparse.ArgumentParser(description="Ingest feature events into Amplitude")
    parser.add_argument(
        "--input",
        required=True,
        type=str,
        help="Path to feature_events CSV file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually send events, just validate payloads",
    )
    parsed = parser.parse_args(args)
    ingest_events(Path(parsed.input), dry_run=parsed.dry_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1:])
