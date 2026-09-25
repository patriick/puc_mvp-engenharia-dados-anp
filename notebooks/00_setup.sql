-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### # 00 - Configuração do Ambiente
-- MAGIC
-- MAGIC Este notebook contém a configuração inicial do ambiente utilizado no MVP de Engenharia de Dados.
-- MAGIC
-- MAGIC A estrutura do projeto foi organizada com base na arquitetura Medalhão, separando os dados em três camadas principais:
-- MAGIC
-- MAGIC - **Bronze**: armazenamento dos dados brutos, preservando o conteúdo original disponibilizado pela fonte;
-- MAGIC - **Silver**: armazenamento dos dados tratados, padronizados e preparados para utilização analítica;
-- MAGIC - **Gold**: armazenamento dos dados modelados e estruturados para consumo e análise.
-- MAGIC
-- MAGIC Nesta etapa também é criado um **Volume** no Databricks para armazenamento dos arquivos brutos provenientes da Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).
-- MAGIC
-- MAGIC ### ## Estrutura criada
-- MAGIC
-- MAGIC - Catálogo: `workspace`
-- MAGIC - Schema Bronze: `workspace.bronze`
-- MAGIC - Schema Silver: `workspace.silver`
-- MAGIC - Schema Gold: `workspace.gold`
-- MAGIC - Volume de arquivos brutos: `workspace.bronze.raw_files`
-- MAGIC
-- MAGIC Essa organização permite separar claramente as diferentes etapas do pipeline e manter a rastreabilidade dos dados desde a origem até sua disponibilização para análise.

-- COMMAND ----------

-- Definição do catálogo principal do projeto
USE CATALOG workspace;

-- COMMAND ----------

-- Criação dos schemas que representam as camadas Bronze, Silver e Gold
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

-- Validação dos schemas criados no catálogo
SHOW SCHEMAS;

-- COMMAND ----------

-- Definição da camada Bronze como schema de trabalho
USE SCHEMA bronze;

-- COMMAND ----------

-- Criação do Volume para armazenamento dos arquivos brutos
CREATE VOLUME IF NOT EXISTS raw_files;

-- COMMAND ----------

-- Validação do Volume destinado aos arquivos brutos
SHOW VOLUMES;/Volumes/workspace/bronze/raw_files