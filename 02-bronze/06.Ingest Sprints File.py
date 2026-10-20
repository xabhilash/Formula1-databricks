# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Sprints file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema (preserve the nested structure)
# MAGIC 3. Add Metadata Columns
# MAGIC     - Source file
# MAGIC     - Ingestion Timestamp
# MAGIC 4. Write to bronze delta table
# MAGIC

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

source_file = f'{landing_folder_path}/sprints'
table_name = f'{catalog_name}.{bronze_schema}.sprints'
table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *
sprints_schema = StructType([
    StructField("date", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("round", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("constructorId", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Cell 8
sprints_df = (spark.read
        .format('json')
        .option('mode', 'FAILFAST')
        .option('multiline', 'true')
        .schema(sprints_schema)
        .load(source_file)
)
sprints_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Metadata column

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)
sprints_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write to bronze delta table

# COMMAND ----------

sprints_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.sprints;
# MAGIC -- display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*) from formula1.bronze.sprints group by season order by season

# COMMAND ----------

