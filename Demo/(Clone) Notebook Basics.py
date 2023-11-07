# Databricks notebook source
print("jello world")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hellow from my world"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title
# MAGIC ## SEcondarly title
# MAGIC ### Smaller title

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

#runs the other notebook, we can then access it's as if we just ran the entire notebook in this notebook
print(full_name)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

#can find the location through the catalog explorer

%fs ls 'dbfs:/user/hive/warehouse/new_default_deeps.db'

# COMMAND ----------

default_db_files = dbutils.fs.ls('dbfs:/user/hive/warehouse/new_default_deeps.db')
print(default_db_files)

# COMMAND ----------

display(default_db_files)

# COMMAND ----------


