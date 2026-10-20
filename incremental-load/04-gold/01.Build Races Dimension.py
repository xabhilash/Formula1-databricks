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

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables
# MAGIC - circuits
# MAGIC - races

# COMMAND ----------

circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits").filter(col('batch_id') == v_batch_id)
races_df = spark.table(f"{catalog_name}.{silver_schema}.races").filter(col('batch_id') == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Join races with circuits using circuit_id
# MAGIC
# MAGIC select the following columns
# MAGIC 1. races.season
# MAGIC 2. races.round
# MAGIC 3. races.race_name
# MAGIC 4. race.race_date
# MAGIC 5. circuits.circuit_name
# MAGIC 6. circuits.locality
# MAGIC 7. circuits.country

# COMMAND ----------

from pyspark.sql.functions import *
display(races_df.select('season'))

# COMMAND ----------

dim_races_df = (races_df
                .join(
                    circuits_df, 
                    races_df.circuit_id == circuits_df.circuit_id, 
                    'inner')
                .select(races_df.season, 
                        races_df.round, 
                        races_df.race_name, 
                        races_df.race_date,
                        circuits_df.circuit_name,
                        circuits_df.locality,
                        circuits_df.country)
                )

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 - Write the transformed data to the gold dim_races table

# COMMAND ----------

write_to_gold(
 input_df=dim_races_df,
 target_table=target_table,
 merge_condition='t.season = s.season and t.round = s.round',
 columns_to_update=[
     'race_name',
     'race_date',
     'circuit_name',
     'locality',
     'country'
 ]   
)

# COMMAND ----------

# dim_races_df.write.format('delta').mode('overwrite').saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

