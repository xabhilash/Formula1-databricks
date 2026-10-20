# Databricks notebook source
dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/04.gold-helpers"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors").filter(col('batch_id') == v_batch_id)
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

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition='t.constructor_id = s.constructor_id',
    columns_to_update=[
        'constructor_name',
        'nationality',
        'nationality_region'
    ]
)

# COMMAND ----------

# dim_constructors_df.write.format('delta').mode("overwrite").option('overwriteSchema', True).saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

