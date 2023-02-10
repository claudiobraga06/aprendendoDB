-- Databricks notebook source
use rfb.sandbox

-- COMMAND ----------

use rfb.sandbox;
create table gastos (key int, descr string, valor decimal(18,2)  );
insert into table gastos values (1,"transporte",1010),(2,"escola",500),(3,"aluguel",200);
delete from gastos where key=2;
DESCRIBE history gastos; 

-- COMMAND ----------

select * from gastos;

-- COMMAND ----------

select * from gastos version as of 1;

-- COMMAND ----------

drop table gastos
