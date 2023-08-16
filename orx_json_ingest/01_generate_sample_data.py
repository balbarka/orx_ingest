# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is provide to create the source file necessary for showing a json ingest pattern. It will create mock data of two TIFF w/ four file types; json, jpeg, xml, tiff.

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /dbfs/tmp/orx_src
# MAGIC rm -rf /dbfs/tmp/orx_src/*
# MAGIC mkdir -p /dbfs/tmp/orx_src/meta
# MAGIC rm -rf /dbfs/tmp/orx_src/meta/*
# MAGIC mkdir -p /dbfs/tmp/orx_tgt
# MAGIC rm -rf /dbfs/tmp/orx_tgt/*

# COMMAND ----------

# MAGIC %sh
# MAGIC # First need to create some page json files & tiff, jpeg to ingest:
# MAGIC
# MAGIC echo """{\"id\": 1, \"val\": 10}""" > /dbfs/tmp/orx_src/F001-01.json
# MAGIC echo """{\"id\": 2, \"val\": 20}""" > /dbfs/tmp/orx_src/F001-02.json
# MAGIC echo """<XML>F001</XML>""" > /dbfs/tmp/orx_src/F001.xml
# MAGIC echo """01010101010101""" > /dbfs/tmp/orx_src/F001-01.jpeg
# MAGIC echo """01010101010101""" > /dbfs/tmp/orx_src/F001-02.jpeg
# MAGIC echo """01010101010101""" > /dbfs/tmp/orx_src/F001.tiff
# MAGIC
# MAGIC echo """{\"id\": 3, \"val\": 30}""" > /dbfs/tmp/orx_src/F002-01.json
# MAGIC echo """{\"id\": 4, \"val\": 40}""" > /dbfs/tmp/orx_src/F002-02.json
# MAGIC echo """<XML>F001</XML>""" > /dbfs/tmp/orx_src/F002.xml
# MAGIC echo """01010101010101""" > /dbfs/tmp/orx_src/F002-01.jpeg
# MAGIC echo """01010101010101""" > /dbfs/tmp/orx_src/F002-02.jpeg
# MAGIC echo """01010101010101""" > /dbfs/tmp/orx_src/F002.tiff
# MAGIC
# MAGIC # Second need to create meta json files in a sub folder:
# MAGIC echo """{
# MAGIC 	\"F001-01\": {
# MAGIC         \"Name\": \"F001-01.jpeg\",
# MAGIC         \"Type\": \"F5500\",
# MAGIC         \"NeedsFix\": \"false\"
# MAGIC 	},
# MAGIC 	\"F001-02\": {
# MAGIC         \"Name\": \"F001-02.jpeg\",
# MAGIC         \"Type\": \"F5500\",
# MAGIC         \"NeedsFix\": \"false\"
# MAGIC 	}
# MAGIC }""" > /dbfs/tmp/orx_src/meta/F001.json
# MAGIC
# MAGIC echo """{
# MAGIC 	\"F002-01\": {
# MAGIC         \"Name\": \"F002-01.jpeg\",
# MAGIC         \"Type\": \"F5500\",
# MAGIC         \"NeedsFix\": \"false\"
# MAGIC 	},
# MAGIC 	\"F002-02\": {
# MAGIC         \"Name\": \"F002-02.jpeg\",
# MAGIC         \"Type\": \"F5500\",
# MAGIC         \"NeedsFix\": \"false\"
# MAGIC 	}
# MAGIC }""" > /dbfs/tmp/orx_src/meta/F002.json

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/tmp/orx_src/meta/F002.json

# COMMAND ----------

display(dbutils.fs.ls('/tmp/orx_src'))

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/tmp/orx_src/*.json

# COMMAND ----------

# MAGIC %md
# MAGIC We'll also want to create the table that we'll be ingesting our json into:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS forms.orx_tgt;
# MAGIC CREATE TABLE forms.orx_tgt (
# MAGIC     meta_id STRING,
# MAGIC     id INT, 
# MAGIC     val INT)
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC It is work knowing that since each of the json files can be read as a jsonl, for convenience you can read the data in this directory (if all json files have the same schema).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example only if there is a shared schema on the json files
# MAGIC -- we can save all the json files into a single fold and use spark native serdes to read the table
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS forms;
# MAGIC DROP TABLE IF EXISTS forms.json_data;
# MAGIC CREATE EXTERNAL TABLE forms.json_data (
# MAGIC     id INT,
# MAGIC     val INT)
# MAGIC USING json
# MAGIC OPTIONS (path="/tmp/orx_src/*.json", multiline=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT input_file_name(), * FROM forms.json_data;

# COMMAND ----------


