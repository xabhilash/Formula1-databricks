# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Races.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC     - Source file
# MAGIC     - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table
# MAGIC

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/races.csv'
table_name = f'{catalog_name}.{bronze_schema}.races'
table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the dataframe reader API

# COMMAND ----------

df = spark.read.format('csv').option('header', True).load(source_file)
df.display()

# COMMAND ----------

from pyspark.sql.types import *

races_schema = StructType(
    [
        StructField('season',      IntegerType()),
        StructField('round',       IntegerType()),
        StructField('url',         StringType()),
        StructField('raceName',    StringType()),
        StructField('date',        DateType()),
        StructField('circuitId',   StringType())
    ]
)

# COMMAND ----------

races_df = (spark.read
        .format('csv')
        .option('header', True)
#        .option('inferschema', True)
        .option('mode', 'FAILFAST')
        .schema(races_schema)
        .load(source_file)
)
races_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Metadata column

# COMMAND ----------

from pyspark.sql import functions as F
races_final_df = add_ingestion_metadata(races_df)
races_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write to bronze delta table

# COMMAND ----------

write_to_bronze(races_final_df, table_name, v_batch_id)

# COMMAND ----------

# races_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.races;

# COMMAND ----------

