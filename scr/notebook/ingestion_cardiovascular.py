# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Dados ELT
# MAGIC
# MAGIC Conjunto de dados de previsão de risco de doenças cardiovasculares

# COMMAND ----------

display(dbutils.fs.ls("/"))
#/O resultado sera 6 linhas

# COMMAND ----------

dbutils.fs.mkdirs("/temp/")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/temp/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extraindo os dados/ Realizando a leitura

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("dbfs:/temp/cardiovascular.csv")

# COMMAND ----------

df.toPandas()

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("General_Health").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Armazenando os dados

# COMMAND ----------

df = df.withColumnRenamed("Height_(cm)","Height_cm").withColumnRenamed("Weight_(kg)","Weight_kg")
                                                               

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", True).partitionBy("General_Health").save("/hospital/rw/cardiovascular")

# COMMAND ----------

# MAGIC %md
# MAGIC ## criando data base e tabela pelo delta location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS db_hospital

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS db_hospital.cardiovascular_disease LOCATION "/hospital/rw/cardiovascular_disease"
