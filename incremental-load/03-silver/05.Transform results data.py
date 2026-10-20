# Databricks notebook source
dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/03.silver-helpers"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = spark.table(bronze_table).filter((col('batch_id') == v_batch_id))

# COMMAND ----------

results_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
results_selected_df = (results_df.select('season', 'round', 'constructorId', 'driverId', 'date', 'raceName', 'grid', 'laps', 'number', 'points', 'position', 'positionText', 'status', 'ingestion_timestamp', 'source_file', "batch_id")
                       .withColumnsRenamed({
                           'constructorId': 'constructor_id',
                           'driverId': 'driver_id',
                           'date': 'race_date',
                           'raceName': 'race_name',
                           'grid': 'grid_position',
                           'laps': 'completed_laps',
                           'number': 'car_number',
                           'position': 'final_position',
                           'positionText': 'final_position_text'
                       })
)

# COMMAND ----------

results_valid_df = (results_selected_df
                    .filter(
                        col('season').isNotNull() & 
                        col('round').isNotNull() & 
                        col('constructor_id').isNotNull() & 
                        col('driver_id').isNotNull()
                    )
                    .dropDuplicates(['season', 'round', 'constructor_id', 'driver_id'])
                    )

# COMMAND ----------

display(results_selected_df.count() - results_valid_df.count())

# COMMAND ----------

results_final_df = results_valid_df.withColumn('race_name', initcap('race_name'))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

write_to_silver(
    input_df=results_final_df,
    target_table=silver_table,
    merge_condition='t.season = s.season and t.round = s.round and t.constructor_id = s.constructor_id and t.driver_id = s.driver_id',
    columns_to_update=[
        'race_name',
        'race_date',
        'grid_position',
        'completed_laps',
        'car_number',
        'points',
        'final_position',
        'final_position_text',
        'status',
        'ingestion_timestamp',
        'source_file',
        'batch_id'
    ]
)

# COMMAND ----------

# results_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

