# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list("demo"))

# COMMAND ----------

print(dbutils.secrets.get(scope="demo",key="storageread"))
