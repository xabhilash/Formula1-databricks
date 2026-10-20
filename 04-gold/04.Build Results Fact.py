# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

results_df = (spark.table(f"{catalog_name}.{silver_schema}.results")
                    .withColumn('session_type', lit('RACE'))
                    .drop('race_date', 'race_name', 'ingestion_timestamp', 'source_file')
              )
sprints_df = (spark.table(f"{catalog_name}.{silver_schema}.sprints")
                    .withColumn('session_type', lit('SPRINT'))
                    .drop('race_date', 'race_name', 'ingestion_timestamp', 'source_file')
              )

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

fact_session_results_df = (results_sprints_df
    .withColumn('is_win', col('final_position') == 1)
    .withColumn('is_podium', col('final_position').between(1, 3))
    .withColumn('has_points', col('points') > 0)
)

# COMMAND ----------

display(fact_session_results_df.filter(col('season') == 2020))

# COMMAND ----------

fact_session_results_df.write.format('delta').mode('overwrite').saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

