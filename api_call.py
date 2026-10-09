import argparse
import gzip
import json
import os
from datetime import datetime, timedelta, timezone

import requests

NYC311_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

VOLUME_PATH = "/Volumes/workspace/default/nyc311_raw_new"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (student-project; nyc311-data-engineering-pipeline)"
}


def upload_to_databricks(local_file, volume_file):

    url = (
        f"{DATABRICKS_HOST}"
        f"/api/2.0/fs/files"
        f"{volume_file}"
    )

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream"
    }

    with open(local_file, "rb") as f:

        r = requests.put(
            url,
            headers=headers,
            data=f,
            timeout=120
        )

    r.raise_for_status()

    print(f"Uploaded to Databricks: {volume_file}")


def fetch_nyc311(load, start_date, end_date, max_rows=50000):

    offset = 0
    batch = 1
    total_rows = 0

    while True:

        params = {
            "$limit": max_rows,
            "$offset": offset,
            "$where": (
                f"created_date >= '{start_date}T00:00:00' "
                f"AND created_date < '{end_date}T00:00:00'"
            ),
            "$order": "created_date, unique_key"
        }

        print(
            f"Requesting {start_date} to {end_date} "
            f"| offset={offset}"
        )

        try:

            r = requests.get(
                NYC311_URL,
                headers=HEADERS,
                params=params,
                timeout=120
            )

            r.raise_for_status()

        except requests.RequestException as e:

            print(f"NYC 311 download failed: {e}")
            raise

        data = r.json()

        if not data:
            break

        filename = (
            f"nyc311_{load}_{start_date}_{end_date}"
            f"_batch_{batch}.json.gz"
        )

        local_file = os.path.join("/tmp", filename)

        volume_file = (
            f"{VOLUME_PATH}/{filename}"
        )

        with gzip.open(
            local_file,
            "wt",
            encoding="utf-8"
        ) as f:

            json.dump(data, f)

        print(
            f"Downloaded {len(data)} rows"
        )

        upload_to_databricks(
            local_file,
            volume_file
        )

        total_rows += len(data)

        if len(data) < max_rows:
            break

        offset += max_rows
        batch += 1

    print(
        f"Completed {load} load: "
        f"{total_rows} total rows"
    )


def fetch_nyc311_incremental(
    days=2,
    max_rows=50000
):

    end = datetime.now(timezone.utc)

    start = end - timedelta(days=days)

    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    fetch_nyc311(
        "incremental",
        start_date,
        end_date,
        max_rows
    )


if __name__ == "__main__":

    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--load",
        choices=["full", "incremental"],
        required=True
    )

    ap.add_argument(
        "--start-date",
        type=str,
        default=None
    )

    ap.add_argument(
        "--end-date",
        type=str,
        default=None
    )

    ap.add_argument(
        "--days",
        type=int,
        default=2
    )

    ap.add_argument(
        "--max-rows",
        type=int,
        default=50000
    )

    a = ap.parse_args()

    if a.load == "full":

        if not a.start_date or not a.end_date:

            ap.error(
                "--start-date and --end-date "
                "are required for full load"
            )

        fetch_nyc311(
            "full",
            a.start_date,
            a.end_date,
            a.max_rows
        )

    else:

        fetch_nyc311_incremental(
            a.days,
            a.max_rows
        )