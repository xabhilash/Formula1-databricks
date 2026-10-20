# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Constructors.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC     - Source file
# MAGIC     - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table
# MAGIC

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

source_file = f'{landing_folder_path}/constructors.json'
table_name = f'{catalog_name}.{bronze_schema}.constructors'
table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the dataframe reader API

# COMMAND ----------

df = spark.read.format('json').load(source_file)
df.display()

# COMMAND ----------

constructors_schema = """constructorId string,
                         name string,
                         nationality string,
                         url string"""

# COMMAND ----------

# DBTITLE 1,Cell 8
constructors_df = (spark.read
        .format('json')
        .option('mode', 'FAILFAST')
        .schema(constructors_schema)
        .load(source_file)
)
constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Metadata column

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)
constructors_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write to bronze delta table

# COMMAND ----------

constructors_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.constructors;

# COMMAND ----------

