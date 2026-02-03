from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.column_utils import normalize_dataframe_columns


@dp.table(
    name="dev.experiment01.customers_raw",
    comment="Bronze layer: Raw customer data ingested from CSV files in volume"
)
def customers():
    """
    Reads CSV files from volume using Auto Loader and loads into customers table.
    Includes metadata columns: ingested_file and ingestion_ts.
    Column names are normalized using standard utility function.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", True)
        .option("headerRows", 1)
        .load("/Volumes/workspace/practice/source_data/dirty_dataset")
        .withColumn("ingested_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    
    # Normalize all column names using utility function
    return normalize_dataframe_columns(df)
