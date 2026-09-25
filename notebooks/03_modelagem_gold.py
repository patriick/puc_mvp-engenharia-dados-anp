# Databricks notebook source
# MAGIC %md
# MAGIC ### # 03 - Modelagem da Camada Gold
# MAGIC
# MAGIC Este notebook realiza a modelagem analítica dos dados de produção de petróleo e gás natural provenientes da camada Silver.
# MAGIC
# MAGIC A camada Gold representa os dados preparados para consumo analítico, utilizando um modelo dimensional em esquema estrela composto por uma tabela fato e dimensões relacionadas.
# MAGIC
# MAGIC Para o escopo deste MVP, são considerados os anos completos entre **2016 e 2025**, evitando a comparação entre anos completos e períodos ainda parciais.
# MAGIC
# MAGIC O modelo será composto pelas seguintes tabelas:
# MAGIC
# MAGIC - `dim_tempo`: informações temporais utilizadas nas análises;
# MAGIC - `dim_localidade`: região e unidade da federação;
# MAGIC - `dim_produto`: petróleo e gás natural e suas respectivas unidades de medida;
# MAGIC - `dim_ambiente`: classificação da produção entre terra e mar;
# MAGIC - `fato_producao`: volumes de produção relacionados às respectivas dimensões.
# MAGIC
# MAGIC As tabelas resultantes serão persistidas no schema `workspace.gold`.

# COMMAND ----------

# Importação das funções utilizadas na modelagem da camada Gold

from pyspark.sql import functions as F

# COMMAND ----------

# Carregamento da tabela consolidada da camada Silver

df_silver = spark.table(
    "workspace.silver.producao_hidrocarbonetos"
)

# COMMAND ----------

# Definição do período de análise entre 2016 e 2025

df_gold_base = (
    df_silver
    .filter(
        (F.col("ano") >= 2016) &
        (F.col("ano") <= 2025)
    )
)

# COMMAND ----------

# Validação da quantidade de registros antes e após o recorte temporal

print("Total Silver:", df_silver.count())
print("Total Gold 2016-2025:", df_gold_base.count())

# COMMAND ----------

# Validação dos limites temporais da base utilizada na camada Gold

df_gold_base.select(
    F.min("data_referencia").alias("primeira_referencia"),
    F.max("data_referencia").alias("ultima_referencia")
).display()

# COMMAND ----------

# Criação da dimensão de tempo

dim_tempo = (
    df_gold_base
    .select(
        "data_referencia",
        "ano",
        "mes",
        "mes_numero"
    )
    .dropDuplicates()
    .orderBy("data_referencia")
)

# COMMAND ----------

# Criação da chave da dimensão de tempo

dim_tempo = (
    dim_tempo
    .withColumn(
        "id_tempo",
        (F.col("ano") * 100 + F.col("mes_numero")).cast("int")
    )
    .select(
        "id_tempo",
        "data_referencia",
        "ano",
        "mes",
        "mes_numero"
    )
)

# COMMAND ----------

# Validação da dimensão de tempo

print("Registros dim_tempo:", dim_tempo.count())
display(dim_tempo)

# COMMAND ----------

# Validação da unicidade da chave da dimensão de tempo

dim_tempo.groupBy("id_tempo") \
    .count() \
    .filter(F.col("count") > 1) \
    .display()

# COMMAND ----------

# Importação do recurso utilizado para geração da chave da dimensão

from pyspark.sql.window import Window

# COMMAND ----------

# Criação da dimensão de localidade

dim_localidade = (
    df_gold_base
    .select(
        "grande_regiao",
        "unidade_federacao"
    )
    .dropDuplicates()
    .orderBy(
        "grande_regiao",
        "unidade_federacao"
    )
)

# COMMAND ----------

# Criação da chave da dimensão de localidade

dim_localidade = (
    dim_localidade
    .withColumn(
        "id_localidade",
        F.abs(
            F.xxhash64(
                "grande_regiao",
                "unidade_federacao"
            )
        )
    )
    .select(
        "id_localidade",
        "grande_regiao",
        "unidade_federacao"
    )
)

# COMMAND ----------

# Validação da dimensão de localidade

print("Registros dim_localidade:", dim_localidade.count())
display(
    dim_localidade.orderBy(
        "grande_regiao",
        "unidade_federacao"
    )
)

# COMMAND ----------

# Criação da dimensão de produto

dim_produto = (
    df_gold_base
    .select(
        "produto",
        "unidade_medida"
    )
    .dropDuplicates()
)

# COMMAND ----------

# Criação da chave da dimensão de produto

dim_produto = (
    dim_produto
    .withColumn(
        "id_produto",
        F.abs(
            F.xxhash64(
                "produto",
                "unidade_medida"
            )
        )
    )
    .select(
        "id_produto",
        "produto",
        "unidade_medida"
    )
)

# COMMAND ----------

# Validação da dimensão de produto

print("Registros dim_produto:", dim_produto.count())
display(dim_produto)

# COMMAND ----------

# Validação da unicidade da chave da dimensão de produto

dim_produto.groupBy("id_produto") \
    .count() \
    .filter(F.col("count") > 1) \
    .display()

# COMMAND ----------

# Criação da dimensão de ambiente de produção

dim_ambiente = (
    df_gold_base
    .select(
        F.col("localizacao").alias("ambiente")
    )
    .dropDuplicates()
)

# COMMAND ----------

# Criação da chave da dimensão de ambiente

dim_ambiente = (
    dim_ambiente
    .withColumn(
        "id_ambiente",
        F.abs(
            F.xxhash64("ambiente")
        )
    )
    .select(
        "id_ambiente",
        "ambiente"
    )
)

# COMMAND ----------

# Validação da dimensão de ambiente

print("Registros dim_ambiente:", dim_ambiente.count())
display(dim_ambiente)

# COMMAND ----------

# Validação da unicidade da chave da dimensão de ambiente

dim_ambiente.groupBy("id_ambiente") \
    .count() \
    .filter(F.col("count") > 1) \
    .display()

# COMMAND ----------

# Validação da granularidade dos registros de produção

duplicidades_fato = (
    df_gold_base
    .groupBy(
        "data_referencia",
        "grande_regiao",
        "unidade_federacao",
        "produto",
        "unidade_medida",
        "localizacao"
    )
    .count()
    .filter(F.col("count") > 1)
)

display(duplicidades_fato)

# COMMAND ----------

# Criação da tabela fato de produção

fato_producao = (
    df_gold_base.alias("f")

    .join(
        dim_tempo
        .select("id_tempo", "data_referencia")
        .alias("t"),
        F.col("f.data_referencia") == F.col("t.data_referencia"),
        "left"
    )

    .join(
        dim_localidade.alias("l"),
        (
            (F.col("f.grande_regiao") == F.col("l.grande_regiao")) &
            (F.col("f.unidade_federacao") == F.col("l.unidade_federacao"))
        ),
        "left"
    )

    .join(
        dim_produto.alias("p"),
        (
            (F.col("f.produto") == F.col("p.produto")) &
            (F.col("f.unidade_medida") == F.col("p.unidade_medida"))
        ),
        "left"
    )

    .join(
        dim_ambiente.alias("a"),
        F.col("f.localizacao") == F.col("a.ambiente"),
        "left"
    )

    .select(
        F.col("t.id_tempo"),
        F.col("l.id_localidade"),
        F.col("p.id_produto"),
        F.col("a.id_ambiente"),
        F.col("f.producao")
    )
)

# COMMAND ----------

# Validação da quantidade de registros da tabela fato

print("Base Gold:", df_gold_base.count())
print("Fato Produção:", fato_producao.count())

# COMMAND ----------

# Validação visual da tabela fato

display(fato_producao.limit(20))

# COMMAND ----------

# Validação de chaves estrangeiras nulas na tabela fato

fato_producao.select(
    F.sum(F.col("id_tempo").isNull().cast("int")).alias("id_tempo"),
    F.sum(F.col("id_localidade").isNull().cast("int")).alias("id_localidade"),
    F.sum(F.col("id_produto").isNull().cast("int")).alias("id_produto"),
    F.sum(F.col("id_ambiente").isNull().cast("int")).alias("id_ambiente")
).display()

# COMMAND ----------

# Persistência da dimensão de tempo na camada Gold

(
    dim_tempo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_tempo")
)

# COMMAND ----------

# Persistência da dimensão de localidade na camada Gold

(
    dim_localidade.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_localidade")
)

# COMMAND ----------

# Persistência da dimensão de produto na camada Gold

(
    dim_produto.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_produto")
)

# COMMAND ----------

# Persistência da dimensão de ambiente na camada Gold

(
    dim_ambiente.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_ambiente")
)

# COMMAND ----------

# Persistência da tabela fato de produção na camada Gold

(
    fato_producao.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.fato_producao")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validação das tabelas persistidas na camada Gold
# MAGIC
# MAGIC SHOW TABLES IN workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validação da quantidade de registros das tabelas da camada Gold
# MAGIC
# MAGIC SELECT 'dim_tempo' AS tabela, COUNT(*) AS registros
# MAGIC FROM workspace.gold.dim_tempo
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_localidade', COUNT(*)
# MAGIC FROM workspace.gold.dim_localidade
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_produto', COUNT(*)
# MAGIC FROM workspace.gold.dim_produto
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_ambiente', COUNT(*)
# MAGIC FROM workspace.gold.dim_ambiente
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fato_producao', COUNT(*)
# MAGIC FROM workspace.gold.fato_producao;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validação do relacionamento entre a tabela fato e as dimensões
# MAGIC
# MAGIC SELECT
# MAGIC     t.ano,
# MAGIC     t.mes,
# MAGIC     l.grande_regiao,
# MAGIC     l.unidade_federacao,
# MAGIC     p.produto,
# MAGIC     p.unidade_medida,
# MAGIC     a.ambiente,
# MAGIC     f.producao
# MAGIC
# MAGIC FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC JOIN workspace.gold.dim_tempo t
# MAGIC     ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC JOIN workspace.gold.dim_localidade l
# MAGIC     ON f.id_localidade = l.id_localidade
# MAGIC
# MAGIC JOIN workspace.gold.dim_produto p
# MAGIC     ON f.id_produto = p.id_produto
# MAGIC
# MAGIC JOIN workspace.gold.dim_ambiente a
# MAGIC     ON f.id_ambiente = a.id_ambiente
# MAGIC
# MAGIC ORDER BY
# MAGIC     t.data_referencia,
# MAGIC     l.unidade_federacao,
# MAGIC     p.produto
# MAGIC
# MAGIC LIMIT 50;