# Databricks notebook source
# MAGIC %md
# MAGIC # Acessando o WAREHOUSE

# COMMAND ----------

pip install databricks-sql-connector

# COMMAND ----------

from databricks import sql

# COMMAND ----------

# server_hostname deve ser obtido em connection details do warehouse
# http_path deve ser obtido em connection datails do warehouse
# access_token deve ser obtido em user settings do usuário.




with sql.connect(server_hostname = "adb-4571612221051173.13.azuredatabricks.net",
                 http_path       = "/sql/1.0/warehouses/339b21f7a3803635",
                 access_token    = "dapid7b50090a580f7f6b7cbd440d4bf8f7c-3") as connection:

    with connection.cursor() as cursor:
        cursor.execute("select * from rfb.sandbox.numeros")
        result = cursor.fetchall()

        for row in result:
          print(row)
 
 

# COMMAND ----------

# MAGIC %md
# MAGIC # Acessando com SQL (usa o SPARK SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC use rfb.sandbox;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from numeros

# COMMAND ----------

# MAGIC %md
# MAGIC # Acessando com SPARK

# COMMAND ----------

# MAGIC %python
# MAGIC # filePath deve ser igual a uma external location.
# MAGIC 
# MAGIC filePath = "abfss://tables@externaltablesdb5.dfs.core.windows.net/"
# MAGIC display(dbutils.fs.ls(filePath))

# COMMAND ----------

# MAGIC %python
# MAGIC filePath = "abfss://tables@externaltablesdb5.dfs.core.windows.net/numeros.csv"
# MAGIC df = spark.read.format("csv").load(filePath)
# MAGIC display (df)

# COMMAND ----------

from pyspark.sql.functions import col, translate,lit,when,avg

# COMMAND ----------

display(df)

# COMMAND ----------

df2 = df.withColumn("_c0",
                    when(
                        col("_c0")==3,lit("sim")).
                        otherwise(lit("não"))
                   )
display (df2)

# COMMAND ----------

df3 =  df2.groupby("_c0").count()
display(df3)

# COMMAND ----------

df2.createOrReplaceTempView('decisao')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from decisao

# COMMAND ----------

display(spark.sql('select * from decisao'))
