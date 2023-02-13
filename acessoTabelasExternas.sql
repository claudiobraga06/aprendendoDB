-- Databricks notebook source
list 'abfss://tables@tabelasexternasdb6.dfs.core.windows.net/grupo1' 

-- COMMAND ----------

list 'abfss://tables@tabelasexternasdb6.dfs.core.windows.net/grupo2' 

-- COMMAND ----------

create external table rfb.db1.numeros1 (cep int) using csv location 'abfss://tables@tabelasexternasdb6.dfs.core.windows.net/grupo1/numeros.csv' 

-- COMMAND ----------

create external table rfb.db1.numeros2 (cep int) using csv location 'abfss://tables@tabelasexternasdb6.dfs.core.windows.net/grupo2/numeros2.csv' 

-- COMMAND ----------

select * from rfb.db1.sandbox
