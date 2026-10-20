# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
drivers_dropped_df = drivers_df.drop('url')

# COMMAND ----------

drivers_dropped_df.display()

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df.withColumnsRenamed({
        'driverId': 'driver_id',
        'dateOfBirth': 'date_of_birth'
    })
)

# COMMAND ----------

drivers_renamed_df.display()

# COMMAND ----------

#drivers_name_df = drivers_renamed_df.withColumn('driver_name', concat('name.givenName', lit(' '), 'name.familyName'))
drivers_name_df = (drivers_renamed_df
                   .withColumn('driver_name', 
                               initcap(concat_ws(' ', 'name.givenName', 'name.familyName')))
                   .drop('name')
                   )
drivers_name_df.display()

# COMMAND ----------

drivers_distinct_df = drivers_name_df.dropDuplicates(['driver_id'])
drivers_distinct_df.display()

# COMMAND ----------

drivers_final_df = (drivers_distinct_df.withColumn('nationality', initcap('nationality')))
drivers_final_df.display()

# COMMAND ----------

drivers_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

#spark.sql(f'select * from {silver_table}').display()
display(spark.table(silver_table))

# COMMAND ----------

