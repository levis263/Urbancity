
import polars as pl
import os
from dotenv import load_dotenv

load_dotenv()

storage_options = {
    "account_name": "cityservicestore",
    "account_key": os.getenv("ACCOUNT_KEY"),
}


def transform_data():

    source_uri = "az://bronze1/servicedata.csv"
    target_uri = "az://silver1/servicedata.parquet"

    # Read Bronze CSV
    df = pl.scan_csv(
        source_uri,
        storage_options=storage_options
    )

    # Column mapping
    column_mapping = {
        "Created Date": "created_date",
        "Closed Date": "closed_date",
        "Problem (formerly Complaint Type)": "problem",
        "Problem Detail (formerly Descriptor)": "problem_detail",
        "Location Type": "location_type",
        "Incident Address": "incident_address",
        "City": "city",
        "Borough": "borough",
        "Latitude": "latitude",
        "Longitude": "longitude"
    }

    # Rename and select columns
    df_refined = df.select([
        pl.col(old).alias(new)
        for old, new in column_mapping.items()
    ])

    # Write refined data to Silver as Parquet
    df_refined.sink_parquet(
        target_uri,
        storage_options=storage_options,
        compression="snappy"
    )

    print(f"Data successfully loaded to {target_uri}")


if __name__ == "__main__":
    transform_data()