# Databricks notebook source
# MAGIC %md
# MAGIC ### # 05 - Análise de Negócio
# MAGIC
# MAGIC Este notebook apresenta as análises realizadas a partir das tabelas modeladas na camada Gold.
# MAGIC
# MAGIC As consultas utilizam o modelo dimensional composto pela tabela `fato_producao` e pelas dimensões de tempo, localidade, produto e ambiente.
# MAGIC
# MAGIC O período analisado compreende os anos completos entre **2016 e 2025**.
# MAGIC
# MAGIC As análises buscam responder às seguintes perguntas de negócio:
# MAGIC
# MAGIC 1. Como evoluiu a produção de petróleo e gás natural no Brasil entre 2016 e 2025?
# MAGIC 2. Quais estados apresentam os maiores volumes de produção de petróleo e de gás natural?
# MAGIC 3. Qual é a participação da produção realizada em terra e no mar ao longo do período?
# MAGIC 4. Como a produção está distribuída entre as grandes regiões brasileiras?
# MAGIC 5. A produção está ficando mais ou menos concentrada nos principais estados produtores?
# MAGIC
# MAGIC Como petróleo e gás natural possuem unidades de medida diferentes, suas análises são realizadas separadamente, evitando a soma direta entre volumes de naturezas distintas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Evolução da Produção de Petróleo e Gás Natural
# MAGIC
# MAGIC A primeira análise busca avaliar como os volumes produzidos evoluíram entre 2016 e 2025. Como petróleo e gás natural são apresentados em unidades de medida diferentes, os produtos são analisados separadamente.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evolução anual da produção de petróleo
# MAGIC
# MAGIC SELECT
# MAGIC     t.ano,
# MAGIC     ROUND(SUM(f.producao) / 1000000, 1) AS producao_milhoes_m3
# MAGIC
# MAGIC FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC JOIN workspace.gold.dim_tempo t
# MAGIC     ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC JOIN workspace.gold.dim_produto p
# MAGIC     ON f.id_produto = p.id_produto
# MAGIC
# MAGIC WHERE p.produto = 'PETRÓLEO'
# MAGIC
# MAGIC GROUP BY
# MAGIC     t.ano
# MAGIC
# MAGIC ORDER BY
# MAGIC     t.ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evolução anual da produção de gás natural
# MAGIC
# MAGIC SELECT
# MAGIC     t.ano,
# MAGIC     ROUND(SUM(f.producao) / 1000, 1) AS producao_milhoes_m3
# MAGIC
# MAGIC FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC JOIN workspace.gold.dim_tempo t
# MAGIC     ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC JOIN workspace.gold.dim_produto p
# MAGIC     ON f.id_produto = p.id_produto
# MAGIC
# MAGIC WHERE p.produto = 'GÁS NATURAL'
# MAGIC
# MAGIC GROUP BY
# MAGIC     t.ano
# MAGIC
# MAGIC ORDER BY
# MAGIC     t.ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Variação percentual anual da produção por produto
# MAGIC
# MAGIC WITH producao_anual AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         t.ano,
# MAGIC         p.produto,
# MAGIC         SUM(f.producao) AS producao_total
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_tempo t
# MAGIC         ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     GROUP BY
# MAGIC         t.ano,
# MAGIC         p.produto
# MAGIC ),
# MAGIC
# MAGIC comparacao AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         produto,
# MAGIC         producao_total,
# MAGIC
# MAGIC         LAG(producao_total) OVER (
# MAGIC             PARTITION BY produto
# MAGIC             ORDER BY ano
# MAGIC         ) AS producao_ano_anterior
# MAGIC
# MAGIC     FROM producao_anual
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     ano,
# MAGIC     produto,
# MAGIC
# MAGIC     ROUND(
# MAGIC         (
# MAGIC             (producao_total - producao_ano_anterior)
# MAGIC             / producao_ano_anterior
# MAGIC         ) * 100,
# MAGIC         1
# MAGIC     ) AS variacao_percentual
# MAGIC
# MAGIC FROM comparacao
# MAGIC
# MAGIC WHERE ano >= 2017
# MAGIC
# MAGIC ORDER BY
# MAGIC     CASE
# MAGIC         WHEN produto = 'PETRÓLEO' THEN 1
# MAGIC         WHEN produto = 'GÁS NATURAL' THEN 2
# MAGIC         ELSE 3
# MAGIC     END,
# MAGIC     ano;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da Pergunta 1
# MAGIC
# MAGIC - Entre 2016 e 2025, a produção de petróleo e gás natural apresentou tendência geral de crescimento.
# MAGIC - A produção de petróleo passou de aproximadamente **146,1 milhões de m³ em 2016 para 218,8 milhões de m³ em 2025**. Apesar do crescimento no período, foram observadas retrações anuais em 2018, 2021 e 2024, seguidas por retomadas posteriores.
# MAGIC - A produção de gás natural apresentou trajetória de crescimento mais contínua, passando de aproximadamente **37.890,5 milhões de m³ em 2016 para 65.421,6 milhões de m³ em 2025**.
# MAGIC - A análise da variação anual evidencia que os dois produtos apresentam comportamentos distintos ao longo do tempo, embora ambos encerrem o período analisado com volumes superiores aos observados em 2016.tamentos distintos entre petróleo e gás natural.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Estados com Maiores Volumes de Produção
# MAGIC
# MAGIC Esta análise identifica os estados que apresentaram os maiores volumes acumulados de produção de petróleo e gás natural entre 2016 e 2025. 
# MAGIC Como os produtos possuem unidades e escalas distintas, os rankings são apresentados separadamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Top 5 Estados Produtores de Petróleo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ranking dos cinco maiores estados produtores de petróleo entre 2016 e 2025
# MAGIC
# MAGIC SELECT
# MAGIC     l.unidade_federacao,
# MAGIC     ROUND(
# MAGIC         SUM(f.producao) / 1000000,
# MAGIC         1
# MAGIC     ) AS producao_milhoes_m3
# MAGIC
# MAGIC FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC JOIN workspace.gold.dim_localidade l
# MAGIC     ON f.id_localidade = l.id_localidade
# MAGIC
# MAGIC JOIN workspace.gold.dim_produto p
# MAGIC     ON f.id_produto = p.id_produto
# MAGIC
# MAGIC WHERE p.produto = 'PETRÓLEO'
# MAGIC
# MAGIC GROUP BY
# MAGIC     l.unidade_federacao
# MAGIC
# MAGIC ORDER BY
# MAGIC     producao_milhoes_m3 DESC
# MAGIC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Top 5 Estados Produtores de Gás Natural

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ranking dos cinco maiores estados produtores de gás natural entre 2016 e 2025
# MAGIC
# MAGIC SELECT
# MAGIC     l.unidade_federacao,
# MAGIC     ROUND(
# MAGIC         SUM(f.producao) / 1000,
# MAGIC         1
# MAGIC     ) AS producao_milhoes_m3
# MAGIC
# MAGIC FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC JOIN workspace.gold.dim_localidade l
# MAGIC     ON f.id_localidade = l.id_localidade
# MAGIC
# MAGIC JOIN workspace.gold.dim_produto p
# MAGIC     ON f.id_produto = p.id_produto
# MAGIC
# MAGIC WHERE p.produto = 'GÁS NATURAL'
# MAGIC
# MAGIC GROUP BY
# MAGIC     l.unidade_federacao
# MAGIC
# MAGIC ORDER BY
# MAGIC     producao_milhoes_m3 DESC
# MAGIC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da Pergunta 2
# MAGIC
# MAGIC - A análise dos volumes acumulados entre 2016 e 2025 evidencia uma forte concentração da produção de petróleo e gás natural em alguns estados brasileiros.
# MAGIC - Na produção de **petróleo**, o **Rio de Janeiro** ocupa a primeira posição com aproximadamente **1.379,4 milhões de m³**, apresentando volume substancialmente superior aos demais estados. Em seguida aparecem **São Paulo**, com aproximadamente **154,1 milhões de m³**, e **Espírito Santo**, com aproximadamente **145,8 milhões de m³**. Rio Grande do Norte e Bahia completam o grupo dos cinco maiores produtores.
# MAGIC - Na produção de **gás natural**, o **Rio de Janeiro** também apresenta o maior volume acumulado, com aproximadamente **307.866,8 milhões de m³**. Na sequência aparecem **São Paulo**, com aproximadamente **57.256,5 milhões de m³**, e **Amazonas**, com aproximadamente **51.214,4 milhões de m³**, seguidos por Espírito Santo e Bahia.
# MAGIC - Os resultados demonstram que o Rio de Janeiro exerce posição de destaque na produção nacional dos dois produtos durante o período analisado, enquanto os demais estados apresentam participações significativamente menores.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Participação da Produção em Terra e no Mar
# MAGIC
# MAGIC Esta análise avalia como a produção de petróleo e gás natural está distribuída entre os ambientes terrestre e marítimo ao longo do período de 2016 a 2025.
# MAGIC
# MAGIC Como os produtos possuem escalas distintas, a participação é calculada separadamente para petróleo e gás natural, utilizando valores percentuais.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Participação Terra × Mar — Petróleo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Participação da produção de petróleo entre os ambientes terrestre e marítimo
# MAGIC
# MAGIC WITH producao_ambiente AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         a.ambiente,
# MAGIC         SUM(f.producao) AS producao_total
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     JOIN workspace.gold.dim_ambiente a
# MAGIC         ON f.id_ambiente = a.id_ambiente
# MAGIC
# MAGIC     WHERE p.produto = 'PETRÓLEO'
# MAGIC
# MAGIC     GROUP BY
# MAGIC         a.ambiente
# MAGIC ),
# MAGIC
# MAGIC total AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(producao_total) AS producao_geral
# MAGIC
# MAGIC     FROM producao_ambiente
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     p.ambiente,
# MAGIC
# MAGIC     ROUND(
# MAGIC         p.producao_total / t.producao_geral * 100,
# MAGIC         1
# MAGIC     ) AS participacao_percentual
# MAGIC
# MAGIC FROM producao_ambiente p
# MAGIC
# MAGIC CROSS JOIN total t
# MAGIC
# MAGIC ORDER BY
# MAGIC     participacao_percentual DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Participação Terra × Mar — Gás Natural

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Participação da produção de gás natural entre os ambientes terrestre e marítimo
# MAGIC
# MAGIC WITH producao_ambiente AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         a.ambiente,
# MAGIC         SUM(f.producao) AS producao_total
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     JOIN workspace.gold.dim_ambiente a
# MAGIC         ON f.id_ambiente = a.id_ambiente
# MAGIC
# MAGIC     WHERE p.produto = 'GÁS NATURAL'
# MAGIC
# MAGIC     GROUP BY
# MAGIC         a.ambiente
# MAGIC ),
# MAGIC
# MAGIC total AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(producao_total) AS producao_geral
# MAGIC
# MAGIC     FROM producao_ambiente
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     p.ambiente,
# MAGIC
# MAGIC     ROUND(
# MAGIC         p.producao_total / t.producao_geral * 100,
# MAGIC         1
# MAGIC     ) AS participacao_percentual
# MAGIC
# MAGIC FROM producao_ambiente p
# MAGIC
# MAGIC CROSS JOIN total t
# MAGIC
# MAGIC ORDER BY
# MAGIC     participacao_percentual DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Evolução da Participação da Produção de Petróleo por Ambiente
# MAGIC
# MAGIC A análise a seguir apresenta a participação percentual anual da produção de petróleo realizada em terra e no mar, permitindo avaliar possíveis mudanças na composição da produção ao longo do período.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evolução anual da participação da produção de petróleo por ambiente
# MAGIC
# MAGIC WITH producao_anual_ambiente AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         t.ano,
# MAGIC         a.ambiente,
# MAGIC         SUM(f.producao) AS producao_ambiente
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_tempo t
# MAGIC         ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     JOIN workspace.gold.dim_ambiente a
# MAGIC         ON f.id_ambiente = a.id_ambiente
# MAGIC
# MAGIC     WHERE p.produto = 'PETRÓLEO'
# MAGIC
# MAGIC     GROUP BY
# MAGIC         t.ano,
# MAGIC         a.ambiente
# MAGIC ),
# MAGIC
# MAGIC producao_anual AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         SUM(producao_ambiente) AS producao_total
# MAGIC
# MAGIC     FROM producao_anual_ambiente
# MAGIC
# MAGIC     GROUP BY
# MAGIC         ano
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     p.ano,
# MAGIC     p.ambiente,
# MAGIC
# MAGIC     ROUND(
# MAGIC         p.producao_ambiente / t.producao_total * 100,
# MAGIC         1
# MAGIC     ) AS participacao_percentual
# MAGIC
# MAGIC FROM producao_anual_ambiente p
# MAGIC
# MAGIC JOIN producao_anual t
# MAGIC     ON p.ano = t.ano
# MAGIC
# MAGIC ORDER BY
# MAGIC     p.ano,
# MAGIC     p.ambiente;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Evolução Anual da Participação da Produção de Gás Natural por Ambiente
# MAGIC
# MAGIC A análise a seguir apresenta a participação percentual anual da produção de gás natural realizada em terra e no mar, permitindo avaliar a evolução da composição da produção ao longo do período.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evolução anual da participação da produção de gás natural por ambiente
# MAGIC
# MAGIC WITH producao_anual_ambiente AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         t.ano,
# MAGIC         a.ambiente,
# MAGIC         SUM(f.producao) AS producao_ambiente
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_tempo t
# MAGIC         ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     JOIN workspace.gold.dim_ambiente a
# MAGIC         ON f.id_ambiente = a.id_ambiente
# MAGIC
# MAGIC     WHERE p.produto = 'GÁS NATURAL'
# MAGIC
# MAGIC     GROUP BY
# MAGIC         t.ano,
# MAGIC         a.ambiente
# MAGIC ),
# MAGIC
# MAGIC producao_anual AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         SUM(producao_ambiente) AS producao_total
# MAGIC
# MAGIC     FROM producao_anual_ambiente
# MAGIC
# MAGIC     GROUP BY
# MAGIC         ano
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     p.ano,
# MAGIC     p.ambiente,
# MAGIC
# MAGIC     ROUND(
# MAGIC         p.producao_ambiente / t.producao_total * 100,
# MAGIC         1
# MAGIC     ) AS participacao_percentual
# MAGIC
# MAGIC FROM producao_anual_ambiente p
# MAGIC
# MAGIC JOIN producao_anual t
# MAGIC     ON p.ano = t.ano
# MAGIC
# MAGIC ORDER BY
# MAGIC     p.ano,
# MAGIC     p.ambiente;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da Pergunta 3
# MAGIC
# MAGIC - A análise evidencia predominância da produção marítima tanto para petróleo quanto para gás natural durante todo o período analisado.
# MAGIC - No caso do petróleo, a participação da produção realizada no mar já era superior a 90% em 2016 e apresentou crescimento ao longo do período, enquanto a participação terrestre apresentou redução gradual. Esse comportamento indica aumento da concentração da produção de petróleo em operações marítimas.
# MAGIC - Para o gás natural, também se observa predominância da produção marítima. A participação do ambiente marinho aumentou ao longo do período, enquanto a produção terrestre perdeu participação relativa.
# MAGIC - Os resultados demonstram, portanto, que a produção nacional dos dois produtos se tornou progressivamente mais concentrada no ambiente marítimo entre 2016 e 2025.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Distribuição da Produção entre as Grandes Regiões Brasileiras
# MAGIC
# MAGIC Esta análise avalia como os volumes acumulados de produção de petróleo e gás natural entre 2016 e 2025 estão distribuídos entre as grandes regiões brasileiras.
# MAGIC
# MAGIC Os produtos são analisados separadamente devido às diferenças de unidade e escala de produção.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Petróleo por região

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição da produção acumulada de petróleo entre as grandes regiões brasileiras
# MAGIC
# MAGIC WITH producao_regiao AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         l.grande_regiao,
# MAGIC         SUM(f.producao) AS producao_total
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_localidade l
# MAGIC         ON f.id_localidade = l.id_localidade
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     WHERE p.produto = 'PETRÓLEO'
# MAGIC
# MAGIC     GROUP BY
# MAGIC         l.grande_regiao
# MAGIC ),
# MAGIC
# MAGIC total AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(producao_total) AS producao_geral
# MAGIC
# MAGIC     FROM producao_regiao
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     r.grande_regiao,
# MAGIC
# MAGIC     ROUND(
# MAGIC         r.producao_total / 1000000,
# MAGIC         1
# MAGIC     ) AS producao_milhoes_m3,
# MAGIC
# MAGIC     ROUND(
# MAGIC         r.producao_total / t.producao_geral * 100,
# MAGIC         1
# MAGIC     ) AS participacao_percentual
# MAGIC
# MAGIC FROM producao_regiao r
# MAGIC
# MAGIC CROSS JOIN total t
# MAGIC
# MAGIC ORDER BY
# MAGIC     producao_milhoes_m3 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Gás natural por região

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição da produção acumulada de gás natural entre as grandes regiões brasileiras
# MAGIC
# MAGIC WITH producao_regiao AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         l.grande_regiao,
# MAGIC         SUM(f.producao) AS producao_total
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_localidade l
# MAGIC         ON f.id_localidade = l.id_localidade
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     WHERE p.produto = 'GÁS NATURAL'
# MAGIC
# MAGIC     GROUP BY
# MAGIC         l.grande_regiao
# MAGIC ),
# MAGIC
# MAGIC total AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(producao_total) AS producao_geral
# MAGIC
# MAGIC     FROM producao_regiao
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     r.grande_regiao,
# MAGIC
# MAGIC     ROUND(
# MAGIC         r.producao_total / 1000,
# MAGIC         1
# MAGIC     ) AS producao_milhoes_m3,
# MAGIC
# MAGIC     ROUND(
# MAGIC         r.producao_total / t.producao_geral * 100,
# MAGIC         1
# MAGIC     ) AS participacao_percentual
# MAGIC
# MAGIC FROM producao_regiao r
# MAGIC
# MAGIC CROSS JOIN total t
# MAGIC
# MAGIC ORDER BY
# MAGIC     producao_milhoes_m3 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da Pergunta 4
# MAGIC
# MAGIC - A distribuição regional da produção evidencia forte concentração na **Região Sudeste** entre 2016 e 2025.
# MAGIC - Na produção de **petróleo**, o Sudeste acumulou aproximadamente **1.679,1 milhões de m³**, correspondendo a cerca de **96,7%** do volume produzido no período. As demais regiões apresentam participação significativamente inferior, com destaque para o Nordeste, seguido pelas regiões Norte e Sul.
# MAGIC - Na produção de **gás natural**, o Sudeste também ocupa a principal posição, com aproximadamente **389.222,1 milhões de m³**, equivalente a cerca de **80,2%** da produção acumulada. A Região Norte apresenta a segunda maior participação, seguida pelo Nordeste, enquanto a Região Sul possui participação residual.
# MAGIC - Os resultados demonstram que a produção brasileira de petróleo e gás natural apresenta elevada concentração regional, especialmente na Região Sudeste, embora o gás natural apresente distribuição relativamente menos concentrada do que o petróleo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Concentração da Produção nos Principais Estados Produtores
# MAGIC
# MAGIC Esta análise avalia se a produção brasileira de petróleo e gás natural se tornou mais ou menos concentrada nos principais estados produtores entre 2016 e 2025.
# MAGIC
# MAGIC Para isso, é calculada, em cada ano e para cada produto, a participação conjunta dos três estados com maior volume de produção em relação ao total nacional.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evolução da concentração da produção nos três maiores estados produtores
# MAGIC
# MAGIC WITH producao_estado_ano AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         t.ano,
# MAGIC         p.produto,
# MAGIC         l.unidade_federacao,
# MAGIC         SUM(f.producao) AS producao_estado
# MAGIC
# MAGIC     FROM workspace.gold.fato_producao f
# MAGIC
# MAGIC     JOIN workspace.gold.dim_tempo t
# MAGIC         ON f.id_tempo = t.id_tempo
# MAGIC
# MAGIC     JOIN workspace.gold.dim_localidade l
# MAGIC         ON f.id_localidade = l.id_localidade
# MAGIC
# MAGIC     JOIN workspace.gold.dim_produto p
# MAGIC         ON f.id_produto = p.id_produto
# MAGIC
# MAGIC     GROUP BY
# MAGIC         t.ano,
# MAGIC         p.produto,
# MAGIC         l.unidade_federacao
# MAGIC ),
# MAGIC
# MAGIC ranking_estados AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         produto,
# MAGIC         unidade_federacao,
# MAGIC         producao_estado,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ano, produto
# MAGIC             ORDER BY producao_estado DESC
# MAGIC         ) AS posicao
# MAGIC
# MAGIC     FROM producao_estado_ano
# MAGIC ),
# MAGIC
# MAGIC producao_total_ano AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         produto,
# MAGIC         SUM(producao_estado) AS producao_total
# MAGIC
# MAGIC     FROM producao_estado_ano
# MAGIC
# MAGIC     GROUP BY
# MAGIC         ano,
# MAGIC         produto
# MAGIC ),
# MAGIC
# MAGIC producao_top3 AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         produto,
# MAGIC         SUM(producao_estado) AS producao_top3
# MAGIC
# MAGIC     FROM ranking_estados
# MAGIC
# MAGIC     WHERE posicao <= 3
# MAGIC
# MAGIC     GROUP BY
# MAGIC         ano,
# MAGIC         produto
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t.ano,
# MAGIC     t.produto,
# MAGIC
# MAGIC     ROUND(
# MAGIC         p.producao_top3 / t.producao_total * 100,
# MAGIC         1
# MAGIC     ) AS participacao_top3_percentual
# MAGIC
# MAGIC FROM producao_total_ano t
# MAGIC
# MAGIC JOIN producao_top3 p
# MAGIC     ON t.ano = p.ano
# MAGIC     AND t.produto = p.produto
# MAGIC
# MAGIC ORDER BY
# MAGIC     CASE
# MAGIC         WHEN t.produto = 'PETRÓLEO' THEN 1
# MAGIC         WHEN t.produto = 'GÁS NATURAL' THEN 2
# MAGIC         ELSE 3
# MAGIC     END,
# MAGIC     t.ano;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da Pergunta 5
# MAGIC
# MAGIC - A análise demonstra aumento da concentração da produção nos três maiores estados produtores entre 2016 e 2025.
# MAGIC - No caso do **petróleo**, os três principais estados já concentravam aproximadamente **93,8%** da produção nacional em 2016. Essa participação aumentou gradualmente ao longo do período, alcançando aproximadamente **97,8% em 2025**. Dessa forma, observa-se um aumento de cerca de **4,0 pontos percentuais** na concentração da produção de petróleo.
# MAGIC - Para o **gás natural**, o aumento foi ainda mais expressivo. A participação conjunta dos três maiores estados passou de aproximadamente **72,7% em 2016 para 90,8% em 2025**, representando um crescimento de cerca de **18,1 pontos percentuais**.
# MAGIC - Apesar de pequenas oscilações em alguns anos, os resultados indicam que a produção dos dois produtos se tornou mais concentrada nos principais estados produtores ao longo do período analisado, especialmente no caso do gás natural.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão Geral da Análise
# MAGIC
# MAGIC - As análises realizadas a partir dos dados modelados na camada Gold permitiram responder às cinco perguntas de negócio definidas no início do MVP.
# MAGIC - Entre 2016 e 2025, foi observada uma trajetória geral de crescimento da produção brasileira de petróleo e gás natural. A produção de petróleo passou de aproximadamente **146,1 milhões de m³ para 218,8 milhões de m³**, enquanto a produção de gás natural também apresentou expansão relevante ao longo do período.
# MAGIC - A distribuição geográfica da produção demonstrou forte concentração em determinados estados, com destaque para o **Rio de Janeiro**, que apresentou os maiores volumes acumulados tanto de petróleo quanto de gás natural.
# MAGIC - A análise por ambiente revelou predominância da produção **marítima** nos dois produtos. Essa participação também aumentou ao longo do período, enquanto a produção terrestre perdeu participação relativa.
# MAGIC - Do ponto de vista regional, a **Região Sudeste** apresentou ampla predominância, concentrando aproximadamente **96,7% da produção acumulada de petróleo** e **80,2% da produção acumulada de gás natural** no período analisado.
# MAGIC - Por fim, a análise da participação dos três maiores estados produtores demonstrou aumento da concentração geográfica da produção. No petróleo, essa participação passou de **93,8% para 97,8%**, enquanto no gás natural passou de **72,7% para 90,8%** entre 2016 e 2025.
# MAGIC - Em conjunto, os resultados mostram um cenário caracterizado pelo crescimento da produção, predominância das operações marítimas e elevada concentração geográfica, principalmente na Região Sudeste e nos principais estados produtores.