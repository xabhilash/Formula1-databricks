# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC     - Source file
# MAGIC     - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

source_file = f'{landing_folder_path}/circuits.csv'
table_name = f'{catalog_name}.{bronze_schema}.circuits'
table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the dataframe reader API

# COMMAND ----------

df = spark.read.format('csv').option('header', True).load(source_file)
df.display()

# COMMAND ----------

from pyspark.sql.types import *

circuits_schema = StructType(
    [
        StructField('circuitId',   StringType()),
        StructField('url',         StringType()),
        StructField('circuitName', StringType()),
        StructField('lat',         DoubleType()),
        StructField('long',        DoubleType()),
        StructField('locality',    StringType()),
        StructField('country',     StringType())
    ]
)

# COMMAND ----------

circuits_df = (spark.read
        .format('csv')
        .option('header', True)
#        .option('inferschema', True)
        .option('mode', 'FAILFAST')
        .schema(circuits_schema)
        .load(source_file)
)
circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Metadata column

# COMMAND ----------

from pyspark.sql import functions as F
circuits_final_df = add_ingestion_metadata(circuits_df)
circuits_final_df.display()

# COMMAND ----------

circuits_final_df.select('_metadata').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write to bronze delta table

# COMMAND ----------

circuits_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.circuits
# MAGIC -- display(spark.table(table_name)) (In Python)

# COMMAND ----------

