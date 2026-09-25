# Databricks notebook source
# MAGIC %md
# MAGIC ### # 04 - Qualidade de Dados
# MAGIC
# MAGIC Este notebook realiza a análise de qualidade dos dados utilizados no pipeline de produção de petróleo e gás natural.
# MAGIC
# MAGIC As verificações são realizadas em diferentes etapas do pipeline, permitindo comparar os dados recebidos na camada Bronze com os dados tratados na camada Silver e modelados na camada Gold.
# MAGIC
# MAGIC As principais dimensões de qualidade avaliadas são:
# MAGIC
# MAGIC - **Completude**: presença de valores nulos ou ausentes;
# MAGIC - **Consistência**: conformidade dos valores com os domínios e formatos esperados;
# MAGIC - **Unicidade**: identificação de registros duplicados na granularidade esperada;
# MAGIC - **Acurácia**: validação de valores incompatíveis com o contexto dos dados;
# MAGIC - **Outliers**: identificação de valores extremos que possam afetar as análises.
# MAGIC
# MAGIC Os problemas identificados são documentados juntamente com os tratamentos aplicados ao longo do pipeline.

# COMMAND ----------

# Importação das funções utilizadas na análise de qualidade

from pyspark.sql import functions as F

# COMMAND ----------

# Carregamento das tabelas utilizadas na análise de qualidade

df_petroleo_bronze = spark.table(
    "workspace.bronze.producao_petroleo_raw"
)

df_gas_bronze = spark.table(
    "workspace.bronze.producao_gas_natural_raw"
)

df_silver = spark.table(
    "workspace.silver.producao_hidrocarbonetos"
)

fato_producao = spark.table(
    "workspace.gold.fato_producao"
)

# COMMAND ----------

# Comparação entre o valor bruto e o valor tratado da produção

exemplos_tratamento = (
    df_petroleo_bronze
    .filter(F.col("`PRODUÇÃO`").contains(","))
    .select(
        F.col("`PRODUÇÃO`").alias("producao_bronze")
    )
    .withColumn(
        "producao_silver",
        F.regexp_replace(
            F.col("producao_bronze"),
            ",",
            "."
        ).cast("decimal(20,3)")
    )
    .limit(10)
)

display(exemplos_tratamento)

# COMMAND ----------

# Análise de completude dos campos da camada Silver

campos_silver = df_silver.columns
total_registros = df_silver.count()

resultado_nulos = []

for campo in campos_silver:
    quantidade_nulos = (
        df_silver
        .filter(F.col(campo).isNull())
        .count()
    )

    percentual_nulos = (
        quantidade_nulos / total_registros * 100
    )

    resultado_nulos.append(
        (
            campo,
            quantidade_nulos,
            round(percentual_nulos, 2)
        )
    )

df_nulos = spark.createDataFrame(
    resultado_nulos,
    [
        "campo",
        "quantidade_nulos",
        "percentual_nulos"
    ]
)

display(df_nulos)

# COMMAND ----------

# Validação dos valores existentes no domínio de produto

display(
    df_silver
    .select("produto")
    .distinct()
    .orderBy("produto")
)

# COMMAND ----------

# Validação dos valores existentes no domínio de localização

display(
    df_silver
    .select("localizacao")
    .distinct()
    .orderBy("localizacao")
)

# COMMAND ----------

# Validação da correspondência entre mês e número do mês

display(
    df_silver
    .select(
        "mes",
        "mes_numero"
    )
    .distinct()
    .orderBy("mes_numero")
)

# COMMAND ----------

# Validação da relação entre produto e unidade de medida

display(
    df_silver
    .select(
        "produto",
        "unidade_medida"
    )
    .distinct()
    .orderBy("produto")
)

# COMMAND ----------

# Identificação de registros com produção negativa

registros_negativos = (
    df_silver
    .filter(F.col("producao") < 0)
)

print(
    "Registros com produção negativa:",
    registros_negativos.count()
)

display(registros_negativos)

# COMMAND ----------

# Identificação de duplicidades na granularidade esperada dos dados

duplicidades = (
    df_silver
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

print(
    "Combinações duplicadas:",
    duplicidades.count()
)
display(duplicidades)

# COMMAND ----------

# Validação da consistência entre ano e data de referência

inconsistencias_ano = (
    df_silver
    .filter(
        F.year("data_referencia") != F.col("ano")
    )
)

print(
    "Inconsistências entre ano e data:",
    inconsistencias_ano.count()
)

# COMMAND ----------

# Validação da consistência entre mês e data de referência

inconsistencias_mes = (
    df_silver
    .filter(
        F.month("data_referencia") != F.col("mes_numero")
    )
)

print(
    "Inconsistências entre mês e data:",
    inconsistencias_mes.count()
)

# COMMAND ----------

# Análise estatística dos volumes de produção por produto

estatisticas_producao = (
    df_silver
    .groupBy(
        "produto",
        "unidade_medida"
    )
    .agg(
        F.count("*").alias("registros"),
        F.min("producao").alias("minimo"),
        F.round(F.avg("producao"), 3).alias("media"),
        F.max("producao").alias("maximo")
    )
)

display(estatisticas_producao)

# COMMAND ----------

# Cálculo dos quartis por produto, unidade da federação e ambiente de produção

quartis_contextuais = (
    df_silver
    .groupBy(
        "produto",
        "unidade_federacao",
        "localizacao"
    )
    .agg(
        F.percentile_approx("producao", 0.25).alias("q1"),
        F.percentile_approx("producao", 0.75).alias("q3")
    )
    .withColumn(
        "iqr",
        F.col("q3") - F.col("q1")
    )
    .withColumn(
        "limite_inferior",
        F.col("q1") - (F.lit(1.5) * F.col("iqr"))
    )
    .withColumn(
        "limite_superior",
        F.col("q3") + (F.lit(1.5) * F.col("iqr"))
    )
)

display(quartis)

# COMMAND ----------

# Identificação de potenciais outliers considerando o contexto produtivo

potenciais_outliers_contextuais = (
    df_silver.alias("d")
    .join(
        quartis_contextuais.alias("q"),
        (
            (F.col("d.produto") == F.col("q.produto")) &
            (F.col("d.unidade_federacao") == F.col("q.unidade_federacao")) &
            (F.col("d.localizacao") == F.col("q.localizacao"))
        ),
        "left"
    )
    .filter(
        (F.col("d.producao") < F.col("q.limite_inferior")) |
        (F.col("d.producao") > F.col("q.limite_superior"))
    )
    .select(
        "d.data_referencia",
        "d.unidade_federacao",
        "d.produto",
        "d.localizacao",
        "d.producao",
        "d.unidade_medida",
        "q.limite_inferior",
        "q.limite_superior"
    )
)

print(
    "Potenciais outliers contextuais:",
    potenciais_outliers_contextuais.count()
)

display(
    potenciais_outliers_contextuais
    .orderBy(F.col("producao").desc())
    .limit(20)
)

# COMMAND ----------

# Validação da integridade das chaves estrangeiras da tabela fato

display(
    fato_producao.select(
        F.sum(
            F.col("id_tempo").isNull().cast("int")
        ).alias("id_tempo"),

        F.sum(
            F.col("id_localidade").isNull().cast("int")
        ).alias("id_localidade"),

        F.sum(
            F.col("id_produto").isNull().cast("int")
        ).alias("id_produto"),

        F.sum(
            F.col("id_ambiente").isNull().cast("int")
        ).alias("id_ambiente")
    )
)

# COMMAND ----------

# Validação da preservação dos registros entre a base Gold e a tabela fato

df_gold_periodo = (
    df_silver
    .filter(
        (F.col("ano") >= 2016) &
        (F.col("ano") <= 2025)
    )
)

print("Registros Silver no período 2016-2025:", df_gold_periodo.count())
print("Registros na fato_producao:", fato_producao.count())

# COMMAND ----------

# Consolidação das principais validações de qualidade dos dados

resultado_validacoes = [
    (
        "Produção negativa",
        df_silver.filter(F.col("producao") < 0).count()
    ),
    (
        "Duplicidades na granularidade esperada",
        duplicidades.count()
    ),
    (
        "Inconsistência entre ano e data de referência",
        inconsistencias_ano.count()
    ),
    (
        "Inconsistência entre mês e data de referência",
        inconsistencias_mes.count()
    )
]

df_validacoes = spark.createDataFrame(
    resultado_validacoes,
    [
        "validacao",
        "quantidade_registros"
    ]
)

display(df_validacoes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ## Conclusão da Análise de Qualidade
# MAGIC
# MAGIC A análise de qualidade demonstrou que os dados apresentam condições adequadas para utilização nas análises propostas pelo MVP.
# MAGIC
# MAGIC Durante o processo foi identificado um problema de formatação na variável de produção dos arquivos originais, que utilizava vírgula como separador decimal. O tratamento foi realizado na camada Silver, com a padronização do separador e conversão do campo para o tipo `decimal(20,3)`.
# MAGIC
# MAGIC As verificações realizadas também demonstraram:
# MAGIC
# MAGIC - ausência de valores nulos nos principais campos analíticos;
# MAGIC - consistência dos domínios de produto, localização e mês;
# MAGIC - correspondência adequada entre produto e unidade de medida;
# MAGIC - ausência de valores negativos de produção;
# MAGIC - ausência de duplicidades na granularidade definida;
# MAGIC - consistência entre os campos temporais;
# MAGIC - integridade das chaves utilizadas no modelo dimensional;
# MAGIC - preservação dos registros durante a modelagem da camada Gold.
# MAGIC
# MAGIC Na análise inicial de outliers pelo método do intervalo interquartil (IQR), foram identificados 2.114 registros potencialmente extremos. Entretanto, essa análise comparava contextos produtivos distintos.
# MAGIC
# MAGIC Após o refinamento do método, considerando conjuntamente produto, unidade da federação e ambiente de produção, foram identificados 274 potenciais outliers.
# MAGIC
# MAGIC Esses registros foram mantidos, pois valores estatisticamente extremos não representam necessariamente erros de qualidade. A produção de petróleo e gás natural possui diferenças estruturais relevantes entre estados e entre ambientes terrestres e marítimos, fazendo com que volumes elevados possam representar características legítimas da atividade produtiva.