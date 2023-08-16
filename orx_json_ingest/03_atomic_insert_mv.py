# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC With the dataframe, `records`, defined, we'll now be able to insert those records into `orx_tgt` & conditionally on insert success, we'll move those records.

# COMMAND ----------

# MAGIC %run ./02_json_reader

# COMMAND ----------

display(records)

# COMMAND ----------

tmp = records.withColumn('rec', F.explode(F.col('records'))) \
           .select(F.col('meta_id'),
                   F.col('rec.*'))

display(tmp)

# COMMAND ----------

def insert_records(records):
    records.withColumn('rec', F.explode(F.col('records'))) \
           .select(F.col('meta_id'),
                   F.col('rec.*')) \
           .write.mode("Append").insertInto('forms.orx_tgt')

# COMMAND ----------

tmp = 

display(tmp)

# COMMAND ----------

from subprocess import run
def move_data_files(records):
    records.select(F.array_union(F.array(F.lit('mv')),
                   F.array_union(F.col('data_file_paths'),
                                 F.array(F.lit('/dbfs/tmp/orx_tgt')))).alias('mv_cmd')) \
                   .foreach(lambda x: run(x.mv_cmd))

# COMMAND ----------

try:
    insert_records(records)
    move_files(records)
except:
    # Logic on partial fails
    pass

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM forms.orx_tgt;

# COMMAND ----------

display(dbutils.fs.ls('/tmp/orx_src'))

# COMMAND ----------

tmp = records.select(F.array_union(F.array(F.lit('mv')),
                   F.array_union(F.col('data_file_paths'),
                                 F.array(F.lit('/dbfs/tmp/orx_tgt')))).alias('mv_cmd'))
display(tmp)

# COMMAND ----------


