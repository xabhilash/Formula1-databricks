# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Results file
# MAGIC 1. Read multiple json files from a directory Results
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

source_file = f'{landing_folder_path}/results'
table_name = f'{catalog_name}.{bronze_schema}.results'
table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the dataframe reader API

# COMMAND ----------

df = spark.read.format('json').load(source_file)
df.display()

# COMMAND ----------

from pyspark.sql.types import *
results_schema = StructType(
    [
        StructField('date', DateType()),
        StructField('raceName', StringType()),
        StructField('round', IntegerType()),
        StructField('season', IntegerType()),
        StructField('url', StringType()),
        StructField('constructorId', StringType()),
        StructField('driverId', StringType()),
        StructField('grid', IntegerType()),
        StructField('laps', IntegerType()),
        StructField('number', IntegerType()),
        StructField('points', FloatType()),
        StructField('position', IntegerType()),
        StructField('positionText', StringType()),
        StructField('status', StringType())
    ]
)

# COMMAND ----------

# DBTITLE 1,Cell 8
results_df = (spark.read
        .format('json')
        .option('mode', 'FAILFAST')
        .schema(results_schema)
        .load(source_file)
)
results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Metadata column

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)
results_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write to bronze delta table

# COMMAND ----------

spark.table(table_name).printSchema()

# COMMAND ----------

results_final_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.results;
# MAGIC -- display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*) from formula1.bronze.results group by season order by season

# COMMAND ----------

