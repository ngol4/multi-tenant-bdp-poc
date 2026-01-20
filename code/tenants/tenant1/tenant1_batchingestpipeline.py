import polars as pl
from pathlib import Path
from coredms.core_dms import CoreDMS  # Assuming CoreDMS is defined in this module
from tenants.tenant_batch import TenantBatch  # Assuming TenantBatch is properly configured
import datetime

# Initialize CoreDMS and logger_batch
core_dms = CoreDMS()
DATA_DIR = "data/azurefunctions-dataset2019"
INDEX_NAME = "tenant1_batch_test1"

# Define expected schema
EXPECTED_COLUMNS = [
    "HashOwner", "HashApp", "HashFunction", "Average", "Count",
    "Minimum", "Maximum",
    "percentile_Average_0", "percentile_Average_1", "percentile_Average_25",
    "percentile_Average_50", "percentile_Average_75", "percentile_Average_99",
    "percentile_Average_100"
]

def process_data(df: pl.DataFrame, logger_batch) -> pl.DataFrame:
    """
    Perform data wrangling: enforce schema, convert types, handle missing values.
    """
    logger_batch.info("Processing data and enforcing schema...")

    # Ensure the required columns exist
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        logger_batch.warning(f"Missing columns detected: {missing_cols}")

    # Select only expected columns (if extra exist, drop them)
    df = df.select([col for col in EXPECTED_COLUMNS if col in df.columns])

    # Convert numeric fields to appropriate types
    numeric_cols = EXPECTED_COLUMNS[3:]  # Excluding the first 3 hash-based ID columns
    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False) for col in numeric_cols
    ])

    # Handle missing values by filling with 0 (or another strategy if needed)
    df = df.fill_null(0)

    return df

def main(tenant):
    df_list = []
    file_paths = []

    for i in range(1, 15):  
        file_name = f"function_durations_percentiles.anon.d{str(i).zfill(2)}.csv"
        file_path = Path(__file__).parent.parent.parent.parent / DATA_DIR / file_name

        tenant.logger_batch.info(f"Checking file path: {file_path}")

        if file_path.exists():
            file_paths.append(file_path)
            tenant.logger_batch.info(f"Processing file: {file_path}")

            try:
                # Use Polars' read_csv for efficient data loading
                temp_df = pl.read_csv(file_path)
                
                # Apply data wrangling
                processed_df = process_data(temp_df, tenant.logger_batch)
                df_list.append(processed_df)

            except Exception as e:
                tenant.logger_batch.error(f"Error reading file {file_path}: {e}")
        else:
            tenant.logger_batch.warning(f"File not found: {file_path}")

    if df_list:
        # Concatenate all DataFrames
        df = pl.concat(df_list)
        tenant.logger_batch.info(f"Final processed DataFrame head:\n{df.head(5)}")

        # Calculate the number of rows
        num_rows = df.height  # Polars DataFrame attribute to get number of rows

        # Convert Polars DataFrame to Pandas DataFrame to calculate memory usage in MB
        df_pandas = df.to_pandas()
        data_size_bytes = df_pandas.memory_usage(deep=True).sum()  # Sum the memory usage of all columns
        data_size_mb = data_size_bytes / (1024 * 1024)  # Convert bytes to MB

        # Add data size and number of rows to metrics
        tenant.metrics["data_size_mb"] = data_size_mb
        tenant.metrics["num_rows"] = num_rows

        tenant.logger_batch.info(f"Data size: {data_size_mb:.2f} MB and Number of rows: {num_rows} for tenant: {tenant.tenant_id}")

        # Convert Polars DataFrame to list of dictionaries for storage
        df_list_of_dicts = df.to_dicts()

        try:
            core_dms.store_data(INDEX_NAME, df_list_of_dicts)
        except Exception as e:
            tenant.logger_batch.error(f"Error storing data in index '{INDEX_NAME}': {e}")
    else:
        tenant.logger_batch.warning("No files processed.")

if __name__ == "__main__":
    # Assuming you have tenant1's logger set up in TenantBatch or somewhere else in the code
    tenant = TenantBatch(tenant_id="tenant1")  # Assuming TenantBatch is available and configured
    main(tenant)
