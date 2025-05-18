import os
from pyspark.sql.functions import col

def transform_data(sdf):
    # Group by realized procedure code, count occurrences
    result = sdf.groupBy("PROC_REA").count()
    return result

def parquet_to_csv(sdf, output_path="output/sih_data.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Create folder if missing
    sdf.toPandas().to_csv(output_path, index=False)
    return output_path
