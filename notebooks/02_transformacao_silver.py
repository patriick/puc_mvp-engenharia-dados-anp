# Databricks notebook source
# MAGIC %md
# MAGIC ### # 02 - Transformação da Camada Silver
# MAGIC
# MAGIC Este notebook realiza a limpeza, padronização e consolidação dos dados de produção de petróleo e gás natural provenientes da camada Bronze.
# MAGIC
# MAGIC Nesta etapa são aplicadas as principais transformações necessárias para tornar os dados consistentes e adequados para utilização analítica.
# MAGIC
# MAGIC Entre os tratamentos realizados estão:
# MAGIC
# MAGIC - padronização dos nomes das colunas;
# MAGIC - conversão dos tipos de dados;
# MAGIC - tratamento do separador decimal da coluna de produção;
# MAGIC - definição das unidades de medida de petróleo e gás natural;
# MAGIC - consolidação das duas bases em uma única estrutura;
# MAGIC - criação do número do mês;
# MAGIC - criação da data de referência mensal;
# MAGIC - validação da quantidade de registros;
# MAGIC - verificação de valores nulos nos principais campos analíticos.
# MAGIC
# MAGIC Os dados tratados são persistidos na tabela:
# MAGIC
# MAGIC - `workspace.silver.producao_hidrocarbonetos`
# MAGIC
# MAGIC A camada Silver mantém o histórico completo disponível na fonte, preservando os dados necessários para etapas posteriores de modelagem e análise.

# COMMAND ----------

# Carregamento das tabelas da camada Bronze

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df_petroleo = spark.table("workspace.bronze.producao_petroleo_raw")
df_gas = spark.table("workspace.bronze.producao_gas_natural_raw")

# COMMAND ----------

# Validação da quantidade de registros carregados

print("Petróleo:", df_petroleo.count())
print("Gás natural:", df_gas.count())

# COMMAND ----------

# Padronização dos nomes de colunas, tipos de dados e unidades de medida
# Também corrige o separador decimal da coluna PRODUÇÃO

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def transformar_silver(df, unidade_medida):

    producao_texto = F.trim(F.col("`PRODUÇÃO`"))

    # Basicamente. se houver vírgula, considera padrão brasileiro:
    # 1.234,56 --> 1234.56
    # 65031,6 --> 65031.6
    producao_padronizada = (
        F.when(
            F.instr(producao_texto, ",") > 0,
            F.regexp_replace(
                F.regexp_replace(producao_texto, r"\.", ""),
                ",",
                "."
            )
        )
        .otherwise(producao_texto)
    )

    return (
        df
        .select(
            F.col("`ANO`").cast("int").alias("ano"),
            F.trim(F.col("`MÊS`")).alias("mes"),
            F.trim(F.col("`GRANDE REGIÃO`")).alias("grande_regiao"),
            F.trim(F.col("`UNIDADE DA FEDERAÇÃO`")).alias("unidade_federacao"),
            F.trim(F.col("`PRODUTO`")).alias("produto"),
            F.trim(F.col("`LOCALIZAÇÃO`")).alias("localizacao"),

            producao_padronizada
                .cast(DecimalType(20, 3))
                .alias("producao"),

            F.lit(unidade_medida).alias("unidade_medida"),

            F.col("_data_ingestao").alias("data_ingestao"),
            F.col("_fonte").alias("fonte"),
            F.col("_arquivo_origem").alias("arquivo_origem")
        )
    )

# COMMAND ----------

# Transformação das bases de petróleo e gás natural para o padrão Silver

df_petroleo_silver = transformar_silver(
    df_petroleo,
    "m3"
)

df_gas_silver = transformar_silver(
    df_gas,
    "mil_m3"
)

# COMMAND ----------

# Visualização de uma amostra dos dados de petróleo após as transformações da camada Silver

display(df_petroleo_silver.limit(20))

# COMMAND ----------

# Validação do schema e os tipos de dados após as transformações da camada Silver

df_petroleo_silver.printSchema()

# COMMAND ----------

# Consolidação das bases de petróleo e gás natural

df_silver = df_petroleo_silver.unionByName(df_gas_silver)
print("Total de registros:", df_silver.count())

# COMMAND ----------

# Padronização do mês e criação do número correspondente

mapa_meses = F.create_map(
    F.lit("JAN"), F.lit(1),
    F.lit("FEV"), F.lit(2),
    F.lit("MAR"), F.lit(3),
    F.lit("ABR"), F.lit(4),
    F.lit("MAI"), F.lit(5),
    F.lit("JUN"), F.lit(6),
    F.lit("JUL"), F.lit(7),
    F.lit("AGO"), F.lit(8),
    F.lit("SET"), F.lit(9),
    F.lit("OUT"), F.lit(10),
    F.lit("NOV"), F.lit(11),
    F.lit("DEZ"), F.lit(12)
)

df_silver = df_silver.withColumn(
    "mes_numero",
    mapa_meses[F.col("mes")]
)

# COMMAND ----------

# Criação da data de referência mensal

df_silver = df_silver.withColumn(
    "data_referencia",
    F.to_date(
        F.concat_ws(
            "-",
            F.col("ano"),
            F.lpad(F.col("mes_numero"), 2, "0"),
            F.lit("01")
        )
    )
)

# COMMAND ----------

# Validação dos campos temporais criados na camada Silver

display(
    df_silver.select(
        "ano",
        "mes",
        "mes_numero",
        "data_referencia",
        "produto",
        "unidade_federacao",
        "localizacao",
        "producao",
        "unidade_medida"
    ).limit(20)
)

# COMMAND ----------

# Validação da quantidade total de registros na camada Silver

print("Total de registros:", df_silver.count())

# COMMAND ----------

# Validação de valores nulos nos principais campos analíticos

df_silver.select(
    [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in [
            "ano",
            "mes",
            "mes_numero",
            "data_referencia",
            "grande_regiao",
            "unidade_federacao",
            "produto",
            "localizacao",
            "producao",
            "unidade_medida"
        ]
    ]
).display()

# COMMAND ----------

# Persistência dos dados tratados e consolidados na camada Silver

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.producao_hidrocarbonetos")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validação da tabela persistida na camada Silver
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     MIN(data_referencia) AS primeira_referencia,
# MAGIC     MAX(data_referencia) AS ultima_referencia
# MAGIC FROM workspace.silver.producao_hidrocarbonetos;