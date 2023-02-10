# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - Acessando com SQL 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 - Usando %SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC use rfb.sandbox;
# MAGIC select c_name from cust_orders limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 - Usando spark.sql

# COMMAND ----------

display(spark.sql(f"SELECT c_name FROM rfb.sandbox.cust_orders limit 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 - Usando Spark

# COMMAND ----------

# filePath pode ser obtido em detalhes da tabela
filePath ="dbfs:/user/hive/warehouse/cust_orders"
df = spark.read.format("delta").load(filePath)
display (df.select("c_name").head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 - Usando o WAREHOUSE
# MAGIC 
# MAGIC Necessário realizar os seguintes passos antes:
# MAGIC - pip install databricks-sql-connector
# MAGIC - from databricks import sql
# MAGIC instala databricks-sql-conector
# MAGIC 
# MAGIC Para o comando CONNECT os seguintes parâmetros devem ser usados:
# MAGIC - server_hostname deve ser obtido em connection details do warehouse
# MAGIC - http_path deve ser obtido em connection datails do warehouse
# MAGIC - access_token deve ser obtido em user settings do usuário.

# COMMAND ----------

pip install databricks-sql-connector

# COMMAND ----------

from databricks import sql

# COMMAND ----------

# server_hostname deve ser obtido em connection details do warehouse
# http_path deve ser obtido em connection datails do warehouse
# access_token deve ser obtido em user settings do usuário.

# pip install databricks-sql-connector
from databricks import sql


with sql.connect(server_hostname = "adb-4571612221051173.13.azuredatabricks.net",
                 http_path       = "/sql/1.0/warehouses/339b21f7a3803635",
                 access_token    = "dapid7b50090a580f7f6b7cbd440d4bf8f7c-3") as connection:

    with connection.cursor() as cursor:
        cursor.execute("SELECT c_name FROM rfb.sandbox.cust_orders")
        result = cursor.fetchmany(5)

        for row in result:
          print(row) 

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 - Acessando tabelas externas com SPARK

# COMMAND ----------

# MAGIC %python
# MAGIC # filePath deve ser igual a uma external location.
# MAGIC 
# MAGIC filePath = "abfss://tables@externaltablesdb5.dfs.core.windows.net/"
# MAGIC display(dbutils.fs.ls(filePath))

# COMMAND ----------

# MAGIC %python
# MAGIC filePathNumeros = filePath + "numeros.csv"
# MAGIC df = spark.read.format("csv").load(filePathNumeros)
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Escrevendo dados em um repositório externo

# COMMAND ----------

df = spark.range(20)


# COMMAND ----------

filePathTab = filePath + "test.delta"

# COMMAND ----------

df.write.format("delta").mode('overwrite').save(filePathTab)

# COMMAND ----------

# MAGIC %sql
# MAGIC list 'abfss://tables@externaltablesdb5.dfs.core.windows.net'

# COMMAND ----------

filePath = "abfss://tables@externaltablesdb5.dfs.core.windows.net/"
display(dbutils.fs.ls(filePath))
