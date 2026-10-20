# Databricks notebook source
dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/03.silver-helpers"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql.functions import *
constructors_df = spark.table(bronze_table).filter((col('batch_id') == v_batch_id))

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

constructors_dropped_df.display()

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df.withColumnsRenamed({
        'constructorId': 'constructor_id',
        'name': 'constructor_name'
    })
)

# COMMAND ----------

constructors_renamed_df.display()

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(['constructor_id'])
constructors_distinct_df.display()

# COMMAND ----------

constructors_final_df = (constructors_distinct_df.withColumn('nationality', initcap('nationality')))
constructors_final_df.display()

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition='t.constructor_id = s.constructor_id',
    columns_to_update=[
        'constructor_name',
        'nationality',
        'ingestion_timestamp',
        'source_file',
        'batch_id'
    ]
)

# COMMAND ----------

# constructors_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

