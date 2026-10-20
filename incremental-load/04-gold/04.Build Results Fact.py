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

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

results_df = (spark.table(f"{catalog_name}.{silver_schema}.results")
                  .filter(col('batch_id') == v_batch_id)
                    .withColumn('session_type', lit('RACE'))
                    .drop('race_date', 'race_name', 'ingestion_timestamp', 'source_file', 'batch_id', 'created_timestamp', 'updated_timestamp')
              )
sprints_df = (spark.table(f"{catalog_name}.{silver_schema}.sprints")
                  .filter(col('batch_id') == v_batch_id)
                    .withColumn('session_type', lit('SPRINT'))
                    .drop('race_date', 'race_name', 'ingestion_timestamp', 'source_file', 'batch_id', 'created_timestamp', 'updated_timestamp')
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

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition="""
        t.season = s.season
        and t.round = s.round
        and t.constructor_id = s.constructor_id
        and t.driver_id = s.driver_id
        and t.session_type = s.session_type
    """,
    columns_to_update=[
        'grid_position',
        'completed_laps',
        'car_number',
        'points',
        'final_position',
        'final_position_text',
        'status',
        'is_win',
        'is_podium',
        'has_points'
    ]
)

# COMMAND ----------

# fact_session_results_df.write.format('delta').mode('overwrite').saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

