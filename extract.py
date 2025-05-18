import os
from pysus.online_data.SIH import SIH
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SIHExtract").getOrCreate()

def extract_data(group: str, state: str, year: int, cache_dir="data_cache"):
    """Extracts SIH data for the given group, state, and year using cache."""
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"sih_{group}_{state}_{year}.parquet")

    if os.path.exists(cache_path):
        print(f"Loading cached data: {cache_path}")
        return spark.read.parquet(cache_path)

    print(f"Downloading new data for {group}, {state}, {year}")
    sih = SIH().load()
    files = sih.get_files(group=group, uf=state, year=year)
    parquet_sets = sih.download(files)

    dfs = []
    for pset in parquet_sets:
        try:
            df = spark.read.parquet(pset.path)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {pset.path}: {e}")

    if not dfs:
        raise ValueError("No data was loaded from the parquet files.")

    full_df = dfs[0]
    for df in dfs[1:]:
        full_df = full_df.unionByName(df, allowMissingColumns=True)

    full_df.write.parquet(cache_path, mode="overwrite")
    return full_df
