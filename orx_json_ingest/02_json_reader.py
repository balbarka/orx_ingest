# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC From a list of files within a batch, we'll transform the files list so that we have a single record for each TIFF. This will help us in case we need to ever filter out an update due to missing files and it will help us keep out ingest atomic. The logic is as follows:
# MAGIC   - PIVOT the source data such that there are now four columns that have arrays of file paths based upon file type.
# MAGIC   - For the json column, iterate through and read all those files into an array struct field, `records`.
# MAGIC   - We'll define this table as `dat` which and the insert / move of all files in this table will be atomic.

# COMMAND ----------

# MAGIC %run ./01_generate_sample_data

# COMMAND ----------

from pyspark.sql import types as T
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import json

# Alt method avoiding filtering

def get_forms_meta(meta_path):
    return spark.read.format("text").option('lineSep','\001').schema("meta STRING").load(meta_path) \
               .withColumn("meta", F.map_entries(F.from_json("meta", "map<string,struct<Name:string,Type:string,NeedsFix:string>>"))) \
               .withColumn("ifn_split", F.split(F.regexp_replace(F.regexp_replace(F.input_file_name(), "dbfs:", "/dbfs"), "%20", " "),'/')) \
               .withColumn("ifn_size", F.size(F.col("ifn_split"))) \
               .withColumn("meta_file_split", F.slice(F.col("ifn_split"),F.col("ifn_size")-1,2)) \
               .withColumn("meta_id", F.element_at(F.split(F.element_at(F.col('meta_file_split'), -1),"\."),1)) \
               .withColumn('data_files', F.array_union(F.flatten(F.transform(F.col('meta.key'), lambda x: F.array(F.concat(x, F.lit('.json')), F.concat(x, F.lit('.jpeg'))))),
                                                       F.array(F.concat(F.col("meta_id"),F.lit('.tiff')),
                                                               F.concat(F.col("meta_id"),F.lit('.xml'))))) \
               .withColumn("data_path", F.concat_ws("/",F.slice(F.col("ifn_split"),1,F.col("ifn_size")-2))) \
               .select("meta_id",
                       F.concat_ws("/",F.col("ifn_split")).alias("meta_file_path"),
                       F.transform(F.col('data_files'), lambda x: F.concat(F.col("data_path"), F.lit("/"), x)).alias("data_file_paths"))

meta = get_forms_meta('/tmp/orx_src/meta/*.json')
display(meta)

# COMMAND ----------

def read_json_file(file):
    with open(file) as f:
        return json.load(f)

udf_schema = T.ArrayType(T.StructType([T.StructField("id", T.IntegerType(), True),
                                       T.StructField("val", T.IntegerType(), True)]))

def read_json_files(files:[str]):
    json_files = [f for f in files if ".json" in f]
    return list(map(read_json_file, json_files))

read_json = spark.udf.register("read_json", read_json_files, udf_schema)
records = meta.withColumn("records", read_json("data_file_paths"))
display(records)
