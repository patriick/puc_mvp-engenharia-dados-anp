-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 - Catálogo de Dados
-- MAGIC
-- MAGIC Este notebook complementa a documentação técnica do MVP por meio da catalogação das principais tabelas e campos utilizados no pipeline.
-- MAGIC
-- MAGIC O catálogo registra o significado das estruturas criadas nas camadas Bronze, Silver e Gold, incluindo descrições das tabelas, campos, tipos de dados, domínios esperados e informações de linhagem.
-- MAGIC
-- MAGIC As descrições também são registradas no Unity Catalog do Databricks, permitindo que as informações permaneçam associadas diretamente aos objetos de dados utilizados no projeto.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 - Documentação da Camada Gold
-- MAGIC
-- MAGIC A camada Gold contém os dados modelados e estruturados para consumo analítico, construídos a partir da tabela consolidada da camada Silver.
-- MAGIC
-- MAGIC Nesta etapa, os dados são organizados em um modelo dimensional no formato de esquema estrela, composto por uma tabela fato e dimensões relacionadas.
-- MAGIC
-- MAGIC As tabelas desta camada são:
-- MAGIC
-- MAGIC - `workspace.gold.dim_tempo`
-- MAGIC - `workspace.gold.dim_localidade`
-- MAGIC - `workspace.gold.dim_produto`
-- MAGIC - `workspace.gold.dim_ambiente`
-- MAGIC - `workspace.gold.fato_producao`
-- MAGIC
-- MAGIC A tabela `fato_producao` armazena os volumes mensais de produção e se relaciona com as dimensões de tempo, localidade, produto e ambiente, permitindo a realização das análises de negócio definidas no MVP.

-- COMMAND ----------

-- Documentação da dimensão de tempo

COMMENT ON TABLE workspace.gold.dim_tempo IS
'Dimensão temporal utilizada nas análises da produção de petróleo e gás natural entre 2016 e 2025. Originada do campo de referência temporal da camada Silver.';

-- COMMAND ----------

-- Documentação dos campos da dimensão de tempo

ALTER TABLE workspace.gold.dim_tempo
ALTER COLUMN id_tempo
COMMENT 'Chave da dimensão de tempo no formato AAAAMM. Exemplo: 202501 representa janeiro de 2025.';

ALTER TABLE workspace.gold.dim_tempo
ALTER COLUMN data_referencia
COMMENT 'Data de referência mensal correspondente ao primeiro dia de cada mês.';

ALTER TABLE workspace.gold.dim_tempo
ALTER COLUMN ano
COMMENT 'Ano de referência da produção. Domínio utilizado no MVP: 2016 a 2025.';

ALTER TABLE workspace.gold.dim_tempo
ALTER COLUMN mes
COMMENT 'Sigla do mês de referência no padrão JAN a DEZ.';


-- COMMAND ----------

-- Documentação da dimensão de localidade

COMMENT ON TABLE workspace.gold.dim_localidade IS
'Dimensão geográfica contendo as unidades da federação e suas respectivas grandes regiões. Originada dos campos geográficos da camada Silver.';

-- COMMAND ----------

-- Documentação dos campos da dimensão de localidade

ALTER TABLE workspace.gold.dim_localidade
ALTER COLUMN id_localidade
COMMENT 'Chave técnica da localidade, gerada a partir da combinação entre grande região e unidade da federação.';

ALTER TABLE workspace.gold.dim_localidade
ALTER COLUMN grande_regiao
COMMENT 'Grande região brasileira associada à unidade da federação.';

ALTER TABLE workspace.gold.dim_localidade
ALTER COLUMN unidade_federacao
COMMENT 'Unidade da Federação associada ao registro de produção.';

-- COMMAND ----------

-- Documentação da dimensão de produto

COMMENT ON TABLE workspace.gold.dim_produto IS
'Dimensão que identifica os produtos analisados e suas respectivas unidades de medida.';

-- COMMAND ----------

-- Documentação dos campos da dimensão de produto

ALTER TABLE workspace.gold.dim_produto
ALTER COLUMN id_produto
COMMENT 'Chave técnica do produto, gerada a partir da combinação entre produto e unidade de medida.';

ALTER TABLE workspace.gold.dim_produto
ALTER COLUMN produto
COMMENT 'Produto energético analisado. Domínio esperado: PETRÓLEO ou GÁS NATURAL.';

ALTER TABLE workspace.gold.dim_produto
ALTER COLUMN unidade_medida
COMMENT 'Unidade original associada ao produto. Petróleo em m3 e gás natural em mil_m3.';

-- COMMAND ----------

-- Documentação da dimensão de ambiente

COMMENT ON TABLE workspace.gold.dim_ambiente IS
'Dimensão que identifica o ambiente onde ocorre a produção de petróleo ou gás natural.';

-- COMMAND ----------

-- Documentação dos campos da dimensão de ambiente

ALTER TABLE workspace.gold.dim_ambiente
ALTER COLUMN id_ambiente
COMMENT 'Chave técnica do ambiente de produção.';

ALTER TABLE workspace.gold.dim_ambiente
ALTER COLUMN ambiente
COMMENT 'Localização da produção. Domínio esperado: TERRA ou MAR.';

-- COMMAND ----------

-- Documentação da tabela fato de produção

COMMENT ON TABLE workspace.gold.fato_producao IS
'Tabela fato que armazena os volumes mensais de produção de petróleo e gás natural e suas respectivas referências às dimensões de tempo, localidade, produto e ambiente.';

-- COMMAND ----------

-- Documentação dos campos da tabela fato de produção

ALTER TABLE workspace.gold.fato_producao
ALTER COLUMN id_tempo
COMMENT 'Chave estrangeira de relacionamento com a dimensão dim_tempo.';

ALTER TABLE workspace.gold.fato_producao
ALTER COLUMN id_localidade
COMMENT 'Chave estrangeira de relacionamento com a dimensão dim_localidade.';

ALTER TABLE workspace.gold.fato_producao
ALTER COLUMN id_produto
COMMENT 'Chave estrangeira de relacionamento com a dimensão dim_produto.';

ALTER TABLE workspace.gold.fato_producao
ALTER COLUMN id_ambiente
COMMENT 'Chave estrangeira de relacionamento com a dimensão dim_ambiente.';

ALTER TABLE workspace.gold.fato_producao
ALTER COLUMN producao
COMMENT 'Volume mensal de produção. A unidade deve ser interpretada por meio da dimensão de produto.';

-- COMMAND ----------

-- Validação da documentação da tabela fato

DESCRIBE TABLE EXTENDED workspace.gold.fato_producao;

-- COMMAND ----------

-- Validação da documentação da dimensão de produto

DESCRIBE TABLE EXTENDED workspace.gold.dim_produto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 - Documentação da Camada Silver
-- MAGIC
-- MAGIC A camada Silver contém os dados de produção de petróleo e gás natural após os processos de limpeza, padronização, tipagem e consolidação realizados a partir das tabelas da camada Bronze.
-- MAGIC
-- MAGIC A tabela `producao_hidrocarbonetos` mantém o histórico completo disponibilizado pela ANP e serve como fonte para a modelagem analítica realizada na camada Gold.

-- COMMAND ----------

-- Documentação da tabela consolidada da camada Silver

COMMENT ON TABLE workspace.silver.producao_hidrocarbonetos IS
'Tabela consolidada contendo os dados tratados e padronizados de produção de petróleo e gás natural provenientes da camada Bronze. Mantém o histórico completo disponibilizado pela ANP e serve como origem para a modelagem da camada Gold.';

-- COMMAND ----------

-- Documentação dos campos da tabela consolidada da camada Silver

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN ano
COMMENT 'Ano de referência da produção, convertido do campo ANO da camada Bronze para tipo inteiro.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN mes
COMMENT 'Sigla do mês de referência da produção. Domínio esperado: JAN a DEZ.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN grande_regiao
COMMENT 'Grande região brasileira associada à unidade da federação.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN unidade_federacao
COMMENT 'Unidade da Federação associada ao registro de produção.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN produto
COMMENT 'Produto analisado. Domínio esperado: PETRÓLEO ou GÁS NATURAL.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN localizacao
COMMENT 'Ambiente em que ocorre a produção. Domínio esperado: TERRA ou MAR.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN producao
COMMENT 'Volume de produção convertido para decimal(20,3). Valores originalmente armazenados como texto tiveram o separador decimal padronizado durante a transformação Silver.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN unidade_medida
COMMENT 'Unidade associada ao volume de produção. Petróleo em m3 e gás natural em mil_m3.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN data_ingestao
COMMENT 'Data e horário em que o dado original foi ingerido na camada Bronze.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN fonte
COMMENT 'Identificação da fonte de origem dos dados. Neste projeto: ANP.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN arquivo_origem
COMMENT 'Nome do arquivo CSV original utilizado na ingestão do registro.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN mes_numero
COMMENT 'Representação numérica do mês de referência. Domínio esperado: valores de 1 a 12.';

ALTER TABLE workspace.silver.producao_hidrocarbonetos
ALTER COLUMN data_referencia
COMMENT 'Data mensal de referência criada a partir dos campos ano e mês, utilizando o primeiro dia de cada mês.';

-- COMMAND ----------

-- Validação da documentação da camada Silver

DESCRIBE TABLE EXTENDED workspace.silver.producao_hidrocarbonetos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 - Documentação da Camada Bronze
-- MAGIC
-- MAGIC A camada Bronze contém os dados brutos de produção de petróleo e gás natural provenientes dos arquivos disponibilizados pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).
-- MAGIC
-- MAGIC Os dados são preservados o mais próximo possível de seu formato original, sendo adicionados apenas metadados técnicos para rastreabilidade da ingestão.
-- MAGIC
-- MAGIC As tabelas desta camada são:
-- MAGIC
-- MAGIC - `workspace.bronze.producao_petroleo_raw`
-- MAGIC - `workspace.bronze.producao_gas_natural_raw`

-- COMMAND ----------

-- Documentação da tabela bruta de produção de petróleo

COMMENT ON TABLE workspace.bronze.producao_petroleo_raw IS
'Tabela da camada Bronze contendo os dados brutos de produção de petróleo disponibilizados pela ANP, preservados o mais próximo possível do formato original da fonte.';

-- COMMAND ----------

-- Documentação dos campos da tabela bruta de produção de petróleo

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `ANO`
COMMENT 'Ano de referência conforme disponibilizado no arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `MÊS`
COMMENT 'Mês de referência conforme disponibilizado no arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `GRANDE REGIÃO`
COMMENT 'Grande região brasileira conforme disponibilizada no arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `UNIDADE DA FEDERAÇÃO`
COMMENT 'Unidade da Federação associada ao registro de produção conforme o arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `PRODUTO`
COMMENT 'Produto informado pela fonte original. Nesta tabela, o domínio esperado é PETRÓLEO.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `LOCALIZAÇÃO`
COMMENT 'Ambiente da produção informado pela fonte original. Domínio esperado: TERRA ou MAR.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `PRODUÇÃO`
COMMENT 'Volume de produção conforme recebido no arquivo original da ANP. Unidade original: m3.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `_data_ingestao`
COMMENT 'Data e horário em que o registro foi ingerido no Databricks.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `_fonte`
COMMENT 'Identificação da fonte de origem dos dados. Neste projeto: ANP.';

ALTER TABLE workspace.bronze.producao_petroleo_raw
ALTER COLUMN `_arquivo_origem`
COMMENT 'Nome do arquivo CSV original utilizado na ingestão do registro.';

-- COMMAND ----------

-- Validação da documentação da tabela bruta de petróleo

DESCRIBE TABLE EXTENDED workspace.bronze.producao_petroleo_raw;

-- COMMAND ----------

-- Documentação da tabela bruta de produção de gás natural

COMMENT ON TABLE workspace.bronze.producao_gas_natural_raw IS
'Tabela da camada Bronze contendo os dados brutos de produção de gás natural disponibilizados pela ANP, preservados o mais próximo possível do formato original da fonte.';

-- COMMAND ----------

-- Documentação dos campos da tabela bruta de produção de gás natural

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `ANO`
COMMENT 'Ano de referência conforme disponibilizado no arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `MÊS`
COMMENT 'Mês de referência conforme disponibilizado no arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `GRANDE REGIÃO`
COMMENT 'Grande região brasileira conforme disponibilizada no arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `UNIDADE DA FEDERAÇÃO`
COMMENT 'Unidade da Federação associada ao registro de produção conforme o arquivo original da ANP.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `PRODUTO`
COMMENT 'Produto informado pela fonte original. Nesta tabela, o domínio esperado é GÁS NATURAL.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `LOCALIZAÇÃO`
COMMENT 'Ambiente da produção informado pela fonte original. Domínio esperado: TERRA ou MAR.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `PRODUÇÃO`
COMMENT 'Volume de produção conforme recebido no arquivo original da ANP. Unidade original: mil m3.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `_data_ingestao`
COMMENT 'Data e horário em que o registro foi ingerido no Databricks.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `_fonte`
COMMENT 'Identificação da fonte de origem dos dados. Neste projeto: ANP.';

ALTER TABLE workspace.bronze.producao_gas_natural_raw
ALTER COLUMN `_arquivo_origem`
COMMENT 'Nome do arquivo CSV original utilizado na ingestão do registro.';

-- COMMAND ----------

-- Validação da documentação da tabela bruta de gás natural

DESCRIBE TABLE EXTENDED workspace.bronze.producao_gas_natural_raw;