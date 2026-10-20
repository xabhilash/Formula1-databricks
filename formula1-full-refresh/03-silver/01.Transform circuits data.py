# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

#circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table)

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitName'), col('lat'), col('long'), col('locality'), col('country'), col('ingestion_timestamp'), col('source_file'))

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

circuits_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

