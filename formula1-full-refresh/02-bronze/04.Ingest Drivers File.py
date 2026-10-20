# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Drivers file
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

source_file = f'{landing_folder_path}/drivers.json'
table_name = f'{catalog_name}.{bronze_schema}.drivers'
table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the dataframe reader API

# COMMAND ----------

df = spark.read.format('json').load(source_file)
df.display()

# COMMAND ----------

from pyspark.sql.types import *
name_schema = StructType(
    [StructField('givenName', StringType()),
    StructField('familyName', StringType())
    ]
)

drivers_schema = StructType(
    [StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())]
)

# COMMAND ----------

# DBTITLE 1,Cell 8
drivers_df = (spark.read
        .format('json')
        .option('mode', 'FAILFAST')
        .schema(drivers_schema)
        .load(source_file)
)
drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Metadata column

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)
drivers_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write to bronze delta table

# COMMAND ----------

drivers_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.drivers;
# MAGIC -- display(spark.table(table_name))

# COMMAND ----------

