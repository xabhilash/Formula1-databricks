# Databricks notebook source
dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/03.silver-helpers"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql.functions import *
races_df = spark.table(bronze_table).filter((col('batch_id') == v_batch_id))

# COMMAND ----------

races_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
races_selected_df = races_df.select(col('season'), col('round'), col('raceName'), col('date'), col('circuitId'), col('ingestion_timestamp'), col('source_file'), col('batch_id'))

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

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition='t.season = s.season and t.round = s.round',
    columns_to_update=[
        'race_name',
        'race_date',
        'circuit_id',
        'ingestion_timestamp',
        'source_file',
        'batch_id'
    ]
)

# COMMAND ----------

# races_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

