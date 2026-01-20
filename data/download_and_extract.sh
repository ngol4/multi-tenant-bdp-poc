#!/bin/bash

# Define variables
URL="https://azurepublicdatasettraces.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz"
TAR_FILE="azurefunctions-dataset2019.tar.xz"
EXTRACT_DIR="azurefunctions-dataset2019"

# Download the file using curl
echo "Downloading dataset..."
curl -L -o $TAR_FILE $URL


# Create the extraction directory if it doesn't exist
echo "Creating extraction directory: $EXTRACT_DIR"
mkdir -p $EXTRACT_DIR

xz -d azurefunctions-dataset2019.tar.xz

mv azurefunctions-dataset2019.tar azurefunctions-dataset2019

cd azurefunctions-dataset2019

tar -xvf *.tar 

rm *.tar

# Remove specific CSV files
echo "Removing unwanted CSV files..."
cd azurefunctions-dataset2019
rm -f invocations_per_function*.csv app_memory_percentiles*.csv

echo "Cleanup complete."