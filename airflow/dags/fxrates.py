import datetime as dt
import io
import os
import zipfile
from pathlib import Path

import clickhouse_connect

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator



# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BASE_DIR = Path('/tmp/airflow/')
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ZIP_OUTPUT_FILE = DATA_DIR / "eurofxref-hist.zip"
OUTPUT_FILE = DATA_DIR / "fx_rates.csv"


dag = DAG(
    dag_id="fx_rates", start_date=dt.datetime(2023, 1, 1), schedule="@daily",
)

def _download_ecb_fx_data(output_path):
    
    # ECB historical reference-rate CSV
    URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"

    # ---------------------------------------------------------
    # Download
    # ---------------------------------------------------------

    print("Downloading ECB historical FX data...")

    response = requests.get(URL, timeout=60)
    response.raise_for_status()

    output_path.write_bytes(response.content)
    print(f"Saved raw ZIP archive to: {output_path}")



download_ecb_fx_data = PythonOperator(
    task_id="download_ecb_fx_data", python_callable=_download_ecb_fx_data, 
    op_kwargs={"output_path": ZIP_OUTPUT_FILE}, dag=dag
)


def _transform_ecb_fx_data(input_path, output_path):

    CURRENCIES = [
        "EUR",
        "NOK",
        "SEK",
        "PLN",
        "RON",
        "DKK",
        "CZK",
    ]
    # ---------------------------------------------------------
    # Read ZIP
    # ---------------------------------------------------------

    with zipfile.ZipFile(input_path) as z:
        # Find the CSV inside the ZIP
        csv_files = [name for name in z.namelist() if name.lower().endswith(".csv")]

        if not csv_files:
            raise RuntimeError("No CSV file found in ECB ZIP archive.")

        csv_name = csv_files[0]

        with z.open(csv_name) as f:
            df = pd.read_csv(f)

    # ---------------------------------------------------------
    # Clean column names
    # ---------------------------------------------------------

    df.columns = [col.strip().upper() for col in df.columns]

    # ECB uses TIME_PERIOD / DATE depending on the file/version.
    date_column = "DATE"

    df[date_column] = pd.to_datetime(df[date_column])

    # ---------------------------------------------------------
    # Keep requested currencies
    # ---------------------------------------------------------

    # EUR is the base currency, so add it explicitly.
    available_currencies = [
        currency for currency in CURRENCIES if currency in df.columns
    ]

    missing = set(CURRENCIES) - set(available_currencies)

    if missing:
        print(f"Warning: currencies not found: {sorted(missing)}")

    df = df[[date_column] + available_currencies]

    # Rename date column
    df = df.rename(columns={date_column: "date"})

    # ---------------------------------------------------------
    # Sort and save
    # ---------------------------------------------------------

    df = df.sort_values("date").reset_index(drop=True)

    df.to_csv(output_path, index=False, float_format="%.8f")

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------

    print()
    print("Done!")
    print(f"Output: {output_path}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {', '.join(df.columns)}")
    print(f"Date range: {df['date'].min().date()} → {df['date'].max().date()}")
    print()
    print(df.head())


transform_ecb_fx_data = PythonOperator(
    task_id="transform_ecb_fx_data", python_callable=_transform_ecb_fx_data, 
    op_kwargs={"input_path": ZIP_OUTPUT_FILE, "output_path": OUTPUT_FILE}, dag=dag
)


def _upload_fx_rates(input_path, **kwargs) -> int:
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "password"),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
    )

    with input_path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        columns = [field for field in reader.fieldnames or []]
        currencies = [field for field in columns if field != "date"]
        rows = []
        for row in reader:
            row_date = date.fromisoformat(row["date"])
            row_values = [1.0, row_date, row_date.year, row_date.month]
            for currency in currencies:
                value = row.get(currency)
                row_values.append(float(value) if value not in (None, "") else None)
            rows.append(tuple(row_values))

    create_columns = [
        "EUR Nullable(Float64)",
        "date Date",
        "YEAR UInt16",
        "MONTH UInt8",
        *[f"{currency} Nullable(Float64)" for currency in currencies],
    ]
    client.command(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    client.command(
        f"""
        CREATE TABLE {TABLE_NAME} (
            {", ".join(create_columns)}
        ) ENGINE = MergeTree
        ORDER BY date
        """
    )
    client.insert(
        TABLE_NAME,
        rows,
        column_names=["EUR", "date", "YEAR", "MONTH", *currencies],
    )
    return len(rows)


upload_fx_rates_task = PythonOperator(
    task_id="upload_fx_rates", python_callable=_upload_fx_rates, op_kwargs={"input_path": OUTPUT_FILE}, dag=dag
)
