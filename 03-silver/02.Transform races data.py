# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

races_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
races_selected_df = races_df.select(col('season'), col('round'), col('raceName'), col('date'), col('circuitId'), col('ingestion_timestamp'), col('source_file'))

# COMMAND ----------

races_selected_df.display()

# COMMAND ----------

races_renamed_df = (
    races_selected_df.withColumnsRenamed({
        'raceName': 'race_name',
        'date': 'race_date',
        'circuitId': 'circuit_id'
    })
)

# COMMAND ----------

races_renamed_df.display()

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(['season', 'round'])
races_distinct_df.display()

# COMMAND ----------

races_final_df = (races_distinct_df.withColumn('race_name', initcap('race_name')))
races_final_df.display()

# COMMAND ----------

races_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

