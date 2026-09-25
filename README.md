# MVP de Engenharia de Dados

## Pipeline de Dados para Análise da Produção de Petróleo e Gás Natural no Brasil

Este projeto foi desenvolvido como parte do MVP da disciplina de Engenharia de Dados e tem como objetivo demonstrar a construção de um pipeline completo de dados, desde a aquisição dos dados brutos até sua disponibilização em uma estrutura analítica adequada para geração de informações de negócio.

O projeto utiliza dados públicos disponibilizados pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) e foi implementado no Databricks Free Edition utilizando arquitetura Medalhão, com as camadas Bronze, Silver e Gold.

A análise de negócio considera os anos completos entre **2016 e 2025**, enquanto as camadas Bronze e Silver preservam o histórico completo disponibilizado pela fonte.

---

# 1. Contexto de Negócio e Perguntas

A produção de petróleo e gás natural possui relevância para o setor energético brasileiro e apresenta diferenças significativas ao longo do tempo, entre regiões, estados e ambientes de produção terrestres e marítimos.

A Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) disponibiliza dados públicos históricos relacionados à produção nacional de petróleo e gás natural. Entretanto, para que esses dados possam ser utilizados de maneira estruturada em análises, é necessário realizar etapas de ingestão, tratamento, padronização, modelagem e validação de qualidade.

Este projeto tem como objetivo construir um pipeline de Engenharia de Dados capaz de organizar esses dados em uma arquitetura analítica, preservando a rastreabilidade desde os arquivos originais até a camada final de consumo.

Para o escopo analítico do MVP, foi selecionado o período entre **2016 e 2025**, permitindo trabalhar com dez anos completos de produção e evitando comparações entre anos completos e o período parcial de 2026.

A partir dos dados processados, foram definidas as seguintes perguntas de negócio:

1. Como evoluiu a produção de petróleo e gás natural no Brasil entre 2016 e 2025?
2. Quais estados apresentam os maiores volumes de produção de petróleo e de gás natural?
3. Qual é a participação da produção realizada em terra e no mar ao longo do período?
4. Como a produção está distribuída entre as grandes regiões brasileiras?
5. A produção está ficando mais ou menos concentrada nos principais estados produtores?

Como petróleo e gás natural são disponibilizados originalmente em unidades de medida distintas, suas análises de volume são realizadas separadamente sempre que necessário.

---

# 2. Fonte e Coleta dos Dados
Os dados utilizados neste projeto foram obtidos no portal oficial de Dados Abertos da Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).

Foram utilizados dois arquivos em formato CSV:

| Conjunto de dados | Arquivo utilizado | Unidade original |
|---|---|---|
| Produção de petróleo | `producao-petroleo-m3-1997-2026.csv` | m³ |
| Produção de gás natural | `producao-gas-natural-1000m3-1997-2026.csv` | mil m³ |

Os dois arquivos possuem estrutura semelhante, contendo os campos:

- `ANO`
- `MÊS`
- `GRANDE REGIÃO`
- `UNIDADE DA FEDERAÇÃO`
- `PRODUTO`
- `LOCALIZAÇÃO`
- `PRODUÇÃO`

Os dados disponibilizados pela ANP são atualizados mensalmente. Embora os arquivos contenham histórico a partir de 1997 e também registros de 2026, o recorte analítico utilizado na camada Gold considera apenas os anos completos entre **2016 e 2025**.

A aquisição dos dados foi realizada por download dos arquivos CSV disponibilizados no portal oficial da ANP. Após o download, os arquivos foram carregados em um Volume do Databricks, preservando os arquivos originais para posterior processamento.

### Fonte oficial
- [Portal de Dados Abertos da ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos)
- [Produção de petróleo e gás natural por estado e localização](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-estado-e-localizacao)
- [Arquivo de produção de petróleo](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/ppgn-el/producao-petroleo-m3.csv/view)
- [Arquivo de produção de gás natural](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/ppgn-el/producao-gas-natural-1000m3.csv/view)

### Licença e utilização dos dados

Os conjuntos utilizados são disponibilizados pela ANP como **dados abertos governamentais**.

De acordo com a Política de Dados Abertos do Poder Executivo Federal, dados abertos são disponibilizados sob licença aberta, permitindo sua utilização, reutilização e cruzamento, sujeitando-se, no máximo, à preservação da autoria ou da fonte.

Neste projeto, a ANP é mantida como fonte de origem dos dados durante todo o pipeline por meio dos metadados adicionados na camada Bronze.

---

# 3. Carga dos Dados

Após o download dos arquivos CSV disponibilizados pela ANP, os dados foram carregados no ambiente Databricks Free Edition.

Para armazenamento dos arquivos originais foi criado um Volume no Unity Catalog:

`workspace.bronze.raw_files`

Nesse Volume foram armazenados os seguintes arquivos:

- `producao-petroleo-m3-1997-2026.csv`
- `producao-gas-natural-1000m3-1997-2026.csv`

A camada Bronze foi utilizada para preservar os dados o mais próximo possível de sua estrutura original. Durante a ingestão, não foram aplicadas transformações de negócio ou padronizações dos campos.

Foram adicionados apenas metadados técnicos para garantir a rastreabilidade dos registros:

- `_data_ingestao`: data e horário da ingestão;
- `_fonte`: identificação da fonte dos dados;
- `_arquivo_origem`: nome do arquivo utilizado na carga.

Após a leitura dos arquivos com PySpark, os dados foram persistidos no formato Delta nas seguintes tabelas:

- `workspace.bronze.producao_petroleo_raw`
- `workspace.bronze.producao_gas_natural_raw`

A quantidade de registros persistidos foi validada após a carga:

| Tabela | Registros |
|---|---:|
| `producao_petroleo_raw` | 7.920 |
| `producao_gas_natural_raw` | 7.731 |
| **Total** | **15.651** |

### Evidência da carga e armazenamento

A imagem abaixo apresenta o Volume utilizado para armazenamento dos arquivos originais e as tabelas persistidas na camada Bronze.

![Estrutura da camada Bronze e arquivos brutos](docs/screenshots/bronze_estrutura_e_arquivos.png)
