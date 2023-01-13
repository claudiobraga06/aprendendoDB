# Databricks notebook source
# MAGIC %md
# MAGIC # Acessando o WAREHOUSE

# COMMAND ----------

pip install databricks-sql-connector

# COMMAND ----------

from databricks import sql

# COMMAND ----------



with sql.connect(server_hostname = "adb-584769158701554.14.azuredatabricks.net",
                 http_path       = "/sql/1.0/warehouses/8258c14297e7f530",
                 access_token    = "dapi7049e84ab4ae1b9fb73cbb4c09f3bc88-3") as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM default.produtos LIMIT 10")
        result = cursor.fetchall()

        for row in result:
          print(row)
 

# COMMAND ----------

# MAGIC %md
# MAGIC # Acessando com SQL (usa o SPARK SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC use default;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from produtos

# COMMAND ----------

# MAGIC %sql
# MAGIC select Categorias,COUNT(*) from produtos group by Categorias

# COMMAND ----------

# MAGIC %md
# MAGIC # Acessando com SPARK

# COMMAND ----------

# MAGIC %python
# MAGIC filePath = "dbfs:/user/hive/warehouse/produtos"
# MAGIC display(dbutils.fs.ls(filePath))

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.format("delta").load(filePath)

# COMMAND ----------

from pyspark.sql.functions import col, translate,lit,when,avg

# COMMAND ----------

df2 = df.withColumn("compar",
                    when(
                        col("segmento")=="Inverno",lit("sim")).
                        otherwise(lit("não"))
                   )

# COMMAND ----------

display(df2)

# COMMAND ----------

df2 =  df.groupby("categorias").count()

# COMMAND ----------

display(df2 )

# COMMAND ----------

df2.createOrReplaceTempView('decisao')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from decisao

# COMMAND ----------

display(spark.sql('select * from decisao'))
