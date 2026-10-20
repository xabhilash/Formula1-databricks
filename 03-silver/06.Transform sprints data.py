# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

sprints_df = spark.table(bronze_table)

# COMMAND ----------

sprints_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
sprints_selected_df = (sprints_df.select('season', 'round', 'constructorId', 'driverId', 'date', 'raceName', 'grid', 'laps', 'number', 'points', 'position', 'positionText', 'status', 'ingestion_timestamp', 'source_file')
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

sprints_valid_df = (sprints_selected_df
                    .filter(
                        col('season').isNotNull() & 
                        col('round').isNotNull() & 
                        col('constructor_id').isNotNull() & 
                        col('driver_id').isNotNull()
                    )
                    .dropDuplicates(['season', 'round', 'constructor_id', 'driver_id'])
                    )

# COMMAND ----------

display(sprints_selected_df.count() - sprints_valid_df.count())

# COMMAND ----------

sprints_final_df = sprints_valid_df.withColumn('race_name', initcap('race_name'))

# COMMAND ----------

sprints_final_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

