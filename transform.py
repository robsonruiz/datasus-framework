import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def parquet_to_csv(df):
    spark.read.parquet(df).write.csv("sih.csv")
    df = pd.read_csv('mypath.csv')
    df.columns = [c.lower() for c in df.columns]
    return df