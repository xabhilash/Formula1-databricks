# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = spark.table(bronze_table)

# COMMAND ----------

results_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
results_selected_df = (results_df.select('season', 'round', 'constructorId', 'driverId', 'date', 'raceName', 'grid', 'laps', 'number', 'points', 'position', 'positionText', 'status', 'ingestion_timestamp', 'source_file')
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

results_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

