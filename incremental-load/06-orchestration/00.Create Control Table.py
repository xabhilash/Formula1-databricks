# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

spark.sql(f'create schema if not exists {catalog_name}.{control_schema}')

# COMMAND ----------

spark.sql(f"""
            create table if not exists {catalog_name}.{control_schema}.batch_control
            (
                batch_id string,
                status string,
                created_timestamp timestamp,
                updated_timestamp timestamp
            )
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.control.batch_control

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from formula1_incr.control.batch_control

# COMMAND ----------

# MAGIC %sql
# MAGIC