-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### # 01 - Ingestão da Camada Bronze
-- MAGIC
-- MAGIC Este notebook realiza a ingestão dos arquivos brutos de produção de petróleo e gás natural disponibilizados pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).
-- MAGIC
-- MAGIC Os arquivos utilizados foram previamente armazenados no Volume `workspace.bronze.raw_files` e são lidos utilizando PySpark.
-- MAGIC
-- MAGIC A camada Bronze tem como objetivo preservar os dados o mais próximo possível de seu formato original. Por esse motivo, nesta etapa não são realizadas transformações de negócio, padronizações de nomes de colunas ou conversões dos tipos originais dos campos.
-- MAGIC
-- MAGIC São adicionados apenas metadados técnicos para garantir a rastreabilidade do processo de ingestão:
-- MAGIC
-- MAGIC - `_data_ingestao`: data e horário de ingestão do registro;
-- MAGIC - `_fonte`: identificação da origem dos dados;
-- MAGIC - `_arquivo_origem`: identificação do arquivo utilizado na carga.
-- MAGIC
-- MAGIC Após a leitura, os dados são persistidos em formato Delta nas seguintes tabelas:
-- MAGIC
-- MAGIC - `workspace.bronze.producao_petroleo_raw`
-- MAGIC - `workspace.bronze.producao_gas_natural_raw`
-- MAGIC
-- MAGIC Ao final do processo, a quantidade de registros persistidos é validada para garantir que não houve perda de dados durante a ingestão.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Importação das funções do PySpark utilizadas na ingestão
-- MAGIC
-- MAGIC from pyspark.sql import functions as F
-- MAGIC catalogo = "workspace"
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Definição dos caminhos dos arquivos brutos armazenados no Volume
-- MAGIC
-- MAGIC caminho_petroleo = "/Volumes/workspace/bronze/raw_files/producao-petroleo-m3-1997-2026.csv"
-- MAGIC caminho_gas = "/Volumes/workspace/bronze/raw_files/producao-gas-natural-1000m3-1997-2026.csv"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Leitura do arquivo bruto de produção de petróleo da ANP
-- MAGIC
-- MAGIC df_petroleo = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("sep", ";")
-- MAGIC     .option("inferSchema", False)
-- MAGIC     .csv(caminho_petroleo)
-- MAGIC )
-- MAGIC
-- MAGIC display(df_petroleo)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Leitura do arquivo bruto de produção de gás natural da ANP
-- MAGIC
-- MAGIC df_gas = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("sep", ";")
-- MAGIC     .option("inferSchema", False)
-- MAGIC     .csv(caminho_gas)
-- MAGIC )
-- MAGIC
-- MAGIC display(df_gas)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Inclusão dos metadados técnicos de rastreabilidade na camada Bronze
-- MAGIC
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC df_petroleo_bronze = (
-- MAGIC     df_petroleo
-- MAGIC     .withColumn("_data_ingestao", F.current_timestamp())
-- MAGIC     .withColumn("_fonte", F.lit("ANP"))
-- MAGIC     .withColumn(
-- MAGIC         "_arquivo_origem",
-- MAGIC         F.lit("producao-petroleo-m3-1997-2026.csv")
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC df_gas_bronze = (
-- MAGIC     df_gas
-- MAGIC     .withColumn("_data_ingestao", F.current_timestamp())
-- MAGIC     .withColumn("_fonte", F.lit("ANP"))
-- MAGIC     .withColumn(
-- MAGIC         "_arquivo_origem",
-- MAGIC         F.lit("producao-gas-natural-1000m3-1997-2026.csv")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Persistência dos dados brutos de petróleo em tabela Delta
-- MAGIC
-- MAGIC (
-- MAGIC     df_petroleo_bronze.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .option("delta.columnMapping.mode", "name")
-- MAGIC     .saveAsTable("workspace.bronze.producao_petroleo_raw")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Persistência dos dados brutos de gás natural em tabela Delta
-- MAGIC
-- MAGIC (
-- MAGIC     df_gas_bronze.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .option("delta.columnMapping.mode", "name")
-- MAGIC     .saveAsTable("workspace.bronze.producao_gas_natural_raw")
-- MAGIC )

-- COMMAND ----------

-- Validação da quantidade de registros persistidos na camada Bronze

SELECT
    (SELECT COUNT(*) FROM workspace.bronze.producao_petroleo_raw) AS petroleo,
    (SELECT COUNT(*) FROM workspace.bronze.producao_gas_natural_raw) AS gas_natural;