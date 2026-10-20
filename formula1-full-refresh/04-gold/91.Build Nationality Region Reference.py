# Databricks notebook source
# MAGIC %run "../00-common/01. env-config"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data = data = [
    # Europe
    ("British", "Europe"),
    ("Italian", "Europe"),
    ("French", "Europe"),
    ("German", "Europe"),
    ("Swiss", "Europe"),
    ("Dutch", "Europe"),
    ("Belgium", "Europe"),
    ("Belgian", "Europe"),
    ("Irish", "Europe"),
    ("Spanish", "Europe"),
    ("Austrian", "Europe"),
    ("East German", "Europe"),
    ("Russian", "Europe"),
    ("Finnish", "Europe"),
    ("Polish", "Europe"),
    ("Portuguese", "Europe"),
    ("Hungarian", "Europe"),
    ("Danish", "Europe"),
    ("Czech", "Europe"),
    ("Liechtensteiner", "Europe"),
    ("Monegasque", "Europe"),
    ("Swedish", "Europe"),
    ("Argentine-italian", "Europe"),
    ("American-italian", "Europe"),

    # North America
    ("American", "North America"),
    ("Canadian", "North America"),
    ("Mexican", "North America"),

    # South America
    ("Brazilian", "South America"),
    ("Chilean", "South America"),
    ("Argentine", "South America"),
    ("Uruguayan", "South America"),
    ("Venezuelan", "South America"),
    ("Colombian", "South America"),

    # Africa
    ("South African", "Africa"),
    ("Rhodesian", "Africa"),

    # Asia
    ("Indian", "Asia"),
    ("Chinese", "Asia"),
    ("Japanese", "Asia"),
    ("Malaysian", "Asia"),
    ("Hong Kong", "Asia"),
    ("Indonesian", "Asia"),
    ("Thai", "Asia"),

    # Oceania
    ("Australian", "Oceania"),
    ("New Zealand", "Oceania"),
    ("New Zealander", "Oceania")
]

columns = ["nationality", "region"]

ref_nationality_region_df = spark.createDataFrame(data, columns)

display(ref_nationality_region_df)

# COMMAND ----------

ref_nationality_region_df.write.format('delta').mode('overwrite').saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

