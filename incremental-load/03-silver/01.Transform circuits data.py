# Databricks notebook source
dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

# MAGIC %run "../00-common/03.silver-helpers"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

#circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table)

# COMMAND ----------

from pyspark.sql.functions import *
circuits_df = spark.table(bronze_table).filter((col('batch_id') == v_batch_id))

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitName'), col('lat'), col('long'), col('locality'), col('country'), col('ingestion_timestamp'), col('source_file'), col('batch_id'))

# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

# circuit_renamed_df = (
#     circuits_selected_df
#     .withColumnRenamed('circuitId', 'circuit_id')
#     .withColumnRenamed('circuitName', 'circuit_name')
#     .withColumnRenamed('lat', 'latitude')
#     .withColumnRenamed('long', 'longitude')
# )

# COMMAND ----------

circuit_renamed_df = (
    circuits_selected_df.withColumnsRenamed({
        'circuitId': 'circuit_id',
        'circuitName': 'circuit_name',
        'lat': 'latitude',
        'long': 'longitude'
    })
)

# COMMAND ----------

circuit_renamed_df.limit(5).display()

# COMMAND ----------

circuits_valid_df = circuit_renamed_df.filter(col("circuit_id").isNotNull())
#circuits_valid_df = circuit_renamed_df.filter("circuit_id IS NOT NULL")
circuits_valid_df.display()

# COMMAND ----------

# circuits_distinct_df = circuits_valid_df.distinct()
# circuits_distinct_df.display()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(['circuit_id'])
circuits_distinct_df.display()

# COMMAND ----------

circuits_final_df = (circuits_distinct_df.withColumn('circuit_name', initcap('circuit_name'))
                                        .withColumn('locality', initcap('locality'))
                     )
circuits_final_df.display()

# COMMAND ----------

# circuits_final_df = (circuits_final_df
#                      .withColumn('created_timestamp', current_timestamp())
#                      .withColumn('updated_timestamp', current_timestamp())
#                      )

# COMMAND ----------

# if not spark.catalog.tableExists(silver_table):
#     (
#         circuits_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)
#     )
# else:

#     from delta.tables import DeltaTable

#     delta_table = DeltaTable.forName(spark, silver_table)
#     (
#         delta_table.alias('t')
#         .merge(
#             circuits_final_df.alias('s'),
#             't.circuit_id = s.circuit_id'
#         )
#         .whenMatchedUpdate(
#             condition='s.batch_id >= t.batch_id',
#             set = {
#                 'circuit_name': 's.circuit_name',
#                 'latitude': 's.latitude',
#                 'longitude': 's.latitude',
#                 'locality': 's.locality',
#                 'country': 's.country',
#                 'ingestion_timestamp': 's.ingestion_timestamp',
#                 'source_file': 's.source_file',
#                 'batch_id': 's.batch_id',
#                 'updated_timestamp': 's.updated_timestamp'
#             }
#         )
#         .whenNotMatchedInsertAll()
#         .execute()

#     )

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition='t.circuit_id = s.circuit_id',
    columns_to_update=[
        'circuit_name',
        'latitude',
        'longitude',
        'locality',
        'country',
        'ingestion_timestamp',
        'source_file',
        'batch_id'
    ]
)

# COMMAND ----------

# circuits_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

