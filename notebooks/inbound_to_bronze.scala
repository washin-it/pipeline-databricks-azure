// Databricks notebook source
// DBTITLE 1,Conferindo Mountpoint
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

// DBTITLE 1,Lendos os dados na camada inbound
val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

// DBTITLE 1,Mostrando os dados
display(dados)

// COMMAND ----------

// DBTITLE 1,Removendo colunas
val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

// DBTITLE 1,Criando coluna de identificação (ID)
import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Salvando na camada bronze
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


