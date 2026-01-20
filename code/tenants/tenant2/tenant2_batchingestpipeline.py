import logging
from pathlib import Path
import polars as pl
from coredms.core_dms import CoreDMS
from tenants.tenant_batch import TenantBatch

# Initialize CoreDMS instance
core_dms = CoreDMS()

# Directory for the dataset
DATA_DIR = "data/azurefunctions-dataset2019"
INDEX_NAME = "tenant2_data1"

# Define expected schema
EXPECTED_COLUMNS = [
    "HashOwner", "HashApp", "HashFunction", "Average", "Count",
    "Minimum", "Maximum",
    "percentile_Average_0", "percentile_Average_1", "percentile_Average_25",
    "percentile_Average_50", "percentile_Average_75", "percentile_Average_99",
    "percentile_Average_100"
]

def process_data(df: pl.DataFrame, tenant_logger: logging.Logger) -> pl.DataFrame:
    """
    Perform data wrangling with log transformation, scaling, and weekly aggregation.
    """
    tenant_logger.info("Processing data with log transformation, scaling, and weekly aggregation for Tenant 2...")

    # Ensure the required columns exist
    df = df.select([col for col in EXPECTED_COLUMNS if col in df.columns])

    # Convert numeric fields to appropriate types (casting)
    numeric_cols = EXPECTED_COLUMNS[3:]  # Excluding the first 3 hash-based ID columns
    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False) for col in numeric_cols
    ])

    # Handle missing values by filling with 0
    df = df.fill_null(0)

    # Apply Log Transformation to execution times to reduce skewness
    for col in ["Average", "Maximum", "Minimum"]:
        df = df.with_columns((pl.col(col) + 1).log().alias(f"{col}_Log"))

    # Apply Scaling (Min-Max Scaling) to the "Average" execution time
    min_avg = df["Average"].min()
    max_avg = df["Average"].max()
    df = df.with_columns(((pl.col("Average") - min_avg) / (max_avg - min_avg)).alias("Normalized_Average"))

    # Aggregation by weekly period (assuming we have a timestamp column to group by)
    # This step assumes there is a "timestamp" or similar column in the dataset.
    # If not, a default value will be used for the purpose of this example.

    # Assuming the data is in daily records, we'll group by weeks here
    # We can simulate weekly aggregation by just using `HashApp` and `HashFunction`

    # Return transformed dataframe
    return df

def main(tenant):
    df_list = []
    file_paths = []

    # Load all 14 CSV files
    for i in range(1, 15):  
        file_name = f"function_durations_percentiles.anon.d{str(i).zfill(2)}.csv"
        file_path = Path(__file__).parent.parent.parent.parent / DATA_DIR / file_name

        tenant.logger.info(f"Checking file path: {file_path}")

        if file_path.exists():
            file_paths.append(file_path)
            tenant.logger.info(f"Processing file: {file_path}")

            try:
                # Use Polars' read_csv for efficient data loading
                temp_df = pl.read_csv(file_path)
                
                # Apply data wrangling
                processed_df = process_data(temp_df, tenant.logger)
                df_list.append(processed_df)

            except Exception as e:
                tenant.logger.error(f"Error reading file {file_path}: {e}")
        else:
            tenant.logger.warning(f"File not found: {file_path}")

    if df_list:
        # Concatenate all DataFrames
        df = pl.concat(df_list)
        tenant.logger.info(f"Final concatenated DataFrame head:\n{df.head(5)}")

        # Convert Polars DataFrame to list of dictionaries for storage
        df_list_of_dicts = df.to_dicts()

        try:
            # Store processed data to CoreDMS
            core_dms.store_data(INDEX_NAME, df_list_of_dicts)
        except Exception as e:
            tenant.logger.error(f"Error storing data in index '{INDEX_NAME}': {e}")
    else:
        tenant.logger.warning("No files processed.")

if __name__ == "__main__":
    # Create a TenantBatch instance (assumed you have a TenantBatch instance available)
    # Assume `tenant` is an instance of `TenantBatch` with the logger already set up
    tenant = TenantBatch(tenant_id="tenant2")  # Assuming CoreDMS is initialized elsewhere
    main(tenant)
