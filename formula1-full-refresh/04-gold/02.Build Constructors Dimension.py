# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructors_df = (constructors_df
                       .join(
                           ref_nationality_region_df, constructors_df.nationality == ref_nationality_region_df.nationality, 
                           'left')
                       .select(
                           constructors_df.constructor_id, constructors_df.constructor_name, constructors_df.nationality, ref_nationality_region_df.region.alias('nationality_region'))
                       )

# COMMAND ----------

dim_constructors_df.write.format('delta').mode("overwrite").option('overwriteSchema', True).saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

