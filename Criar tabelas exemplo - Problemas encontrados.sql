-- Databricks notebook source
-- MAGIC %md
-- MAGIC # erro de gravação SPARK

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Por algum motivo desconhecido, não está sendo possível criar tabelas que não sejam as externas dentro do catalogo RFB.
-- MAGIC Funciona apenas quando utilizo o catalogo hive_metastore

-- COMMAND ----------

# a leitura funciona normalmente
use rfb.db1;
select * from sandbox

-- COMMAND ----------

-- Comando funciona normalmente pois é uma tabela externa
create external table rfb.db1.numeros2 (cep int) using csv location 'abfss://tables@tabelasexternasdb6.dfs.core.windows.net/grupo2/numeros2.csv' 

-- COMMAND ----------

-- Comando não funciona
create table rfb.sandbox.aaa as select * from rfb.db1.numeros2

-- COMMAND ----------

-- Comando funciona normalmente, pois destino é o hive_metastore
create or replace table hive_metastore.teste.numero as select * from rfb.db1.numeros2 

-- COMMAND ----------

select * from hive_metastore.teste.numero

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Criação de tabelas exemplo - Só funciona no WAREHOUSE, não funciona SPARK !

-- COMMAND ----------

create table if not exists rfb.db1.customer as select * from samples.tpch.customer;
create table if not exists rfb.db1.orders as select * from samples.tpch.orders;
create table if not exists rfb.db1.lineitem as select * from samples.tpch.lineitem;


-- COMMAND ----------

use rfb.sandbox;
create or replace table customer_household as
select * from rfb.db1.customer where c_mktsegment='HOUSEHOLD';

-- COMMAND ----------

use rfb.sandbox;
create or replace table orders_household as
select * from rfb.db1.orders where o_custkey in (select c_custkey from customer_household);

-- COMMAND ----------

use rfb.sandbox;
create or replace table cust_orders as
select a.c_name,a.c_address,a.c_phone,b.o_orderkey,b.o_orderdate,b.o_orderstatus,b.o_totalprice from customer_household a 
join orders_household b 
where a.c_custkey=b.o_custkey;


create or replace table lineitem as
select * from rfb.db1.lineitem a where a.l_orderkey in (select o_orderkey from cust_orders);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Criação de outras tabelas

-- COMMAND ----------

list 'abfss://tables@externaltablesdb5.dfs.core.windows.net'


-- COMMAND ----------

create external table if not exists rfb.sandbox.numeros (cep int) using csv location 'abfss://tables@externaltablesdb5.dfs.core.windows.net/numeros.csv';

create external table if not exists rfb.sandbox.area1b  using parquet location 'abfss://tables@externaltablesdb5.dfs.core.windows.net/area1b';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Exemplo de timetravel

-- COMMAND ----------

create table gastos (key int, descr string, valor decimal(18,2)  );
insert into table gastos values (1,"transporte",1010),(2,"escola",500),(3,"aluguel",200);
delete from gastos where key=2;
DESCRIBE history gastos; 
select * from gastos;
select * from gastos version as of 1;
vacuum gastos; -- Vai apagar informações históricas com mais de 1 semana
drop table gastos;
