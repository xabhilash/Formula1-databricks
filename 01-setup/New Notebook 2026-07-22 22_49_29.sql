-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set up Project environment for Formula 1 project

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create External location databricks-course-ext-dl1-formula1

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create an external location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1
    URL 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databricks-course-sc`)
    COMMENT 'External location for formula1 container';

-- COMMAND ----------

show catalogs

-- COMMAND ----------

create catalog if not exists formula1
 managed location 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/'
 comment 'This is the catalog for the formula1 project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

create schema if not exists formula1.landing;
create schema if not exists formula1.bronze
 managed location 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/bronze';
create schema if not exists formula1.silver
 managed location 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/silver';
create schema if not exists formula1.gold
 managed location 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/gold';

-- COMMAND ----------

use catalog formula1;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create volume files

-- COMMAND ----------

create external volume formula1.landing.files
location 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickscoursef1extdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

