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

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/circuits.csv'
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

write_to_bronze(circuits_final_df, table_name, v_batch_id)

# COMMAND ----------

# circuits_final_df = circuits_final_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

# (circuits_final_df.write.format('delta')
# .mode('overwrite')
# .partitionBy('batch_id')
# .option('replaceWhere', f"batch_id = '{v_batch_id}'")
# .saveAsTable(table_name))

# COMMAND ----------

# select * from formula1_incr.bronze.circuits
display(spark.table(table_name))

# COMMAND ----------

