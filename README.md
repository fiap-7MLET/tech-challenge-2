# Pipeline Batch Bovespa: ingestão e arquitetura de dados

Projeto desenvolvido como Tech Challenge (FIAP/Alura) com o objetivo de construir um pipeline batch de dados end-to-end para extração, processamento e análise de dados de ações ou índices da B3 (Bovespa), utilizando serviços da AWS.

O pipeline cobre desde a ingestão dos dados até a disponibilização para consulta analítica, seguindo boas práticas de arquitetura de dados em nuvem.

## Objetivos
1. Realizar a extração (scraping) de dados de ações ou índices da B3, com granularidade diária, utilizando a biblioteca yfinance.
2. Ingerir os dados brutos no AWS S3 em formato Parquet, organizados com particionamento diário (camada raw).
3. Configurar o bucket S3 para acionar uma função Lambda, responsável por iniciar o processamento dos dados.
4. Implementar a Lambda com responsabilidade exclusiva de disparar o Job de ETL no AWS Glue, sem realizar processamento pesado.
5. Executar o processamento no AWS Glue (via código), contendo obrigatoriamente as seguintes transformações:
  - Agrupamento numérico com sumarização (ex: média, soma ou contagem)
  - Renomeação de pelo menos duas colunas existentes (além das colunas de agrupamento)
  - Cálculo baseado em data, como média móvel, diferença entre períodos ou análise de extremos
6. Persistir os dados refinados no S3 em formato Parquet, na pasta refined, com particionamento por data e por código/nome da ação ou índice.
7. Catalogar automaticamente os dados processados no AWS Glue Data Catalog, criando a tabela em um banco de dados (workspace_db).
8. Disponibilizar os dados para consulta via SQL, utilizando o AWS Athena.

## Tecnologias Utilizadas
- Python 3.14 / pyspark
- yfinance (extração de dados financeiros)
- AWS S3 (Data Lake)
- AWS Lambda (ingestão)
- AWS Glue (catálogo e processamento)
- AWS Athena (consulta analítica)
- Docker (empacotamento de dependências)
- Amazon ECR (repositório de imagens)

## Arquitetura

A extração dos dados é realizada utilizando a biblioteca yfinance, com persistência inicial no S3 em partições diárias (camada raw). Após o processamento e aplicação de regras de negócio e cálculos adicionais, os dados são gravados na camada refined, prontos para consumo analítico.

<img width="859" height="471" alt="Tech Challenge  Fase 2 drawio" src="https://github.com/user-attachments/assets/8acaab2f-f222-4e51-916c-80511e84c5ce" />

## Estrutura do Projeto

Neste diretório estão concentrados os scripts utilizados tanto para a construção da imagem Docker da Lambda quanto para o Job de ETL no AWS Glue.

´´´
.
├── ingest/
│ ├── .dockerignore
│ ├── dockerfile
│ ├── data_handler.py
│ ├── glue_job.py
│ └── requirements.txt
├── .gitignore
└── README.md
´´´

Observação: todos os arquivos abaixo são utilizados na imagem Docker com exceção do glue_job.py, que corresponde exclusivamente ao script executado no AWS Glue para aplicação das transformações de ETL e gravação dos dados refinados.

## Criação das pastas no S3
Foram criados dois buckets no Amazon S3, seguindo a separação por camadas do Data Lake:

- **tech-challenge-raw:** responsável por armazenar os dados brutos. Contém a pasta raw/, onde os arquivos Parquet são salvos particionados por índice e data, exatamente como extraídos, sem regras de negócio.

- **tech-challenge-refined:** responsável por armazenar os dados processados. Este bucket contém:
  - **refined/:** dados tratados e enriquecidos pelo job do Glue, particionados conforme definido no ETL;
  - **enriched/:** tabela agregada com análises mensais e métricas derivadas;
  - **query-results/:** resultados das consultas executadas no Athena.

## Lambda para Ingestão de Dados

Na pasta **ingest/** estão os artefatos necessários para a criação da imagem Docker utilizada pelo AWS Lambda responsável pela ingestão dos dados (exceto o script *glue_job*, usado para o ETL na AWS Glue).

Essa abordagem é necessária devido às limitações do Lambda tradicional (ZIP), especialmente quando se trabalha com bibliotecas como numpy e pyarrow.

### data_handler.py

no **data_handler.py** estao os métodos para criar o arquivo em formato parquet no s3. O bucket e a pasta "raw" do S3 estão descritos como variáveis no começo do arquivo.

### docker

Devido a restrições da estrutura do lambda e a utilização de bibliotecas como numpy e pyarrow, é preciso criar uma imagem base com as dependencias necessárias e usar essa imagem para o lambda. o upload de zip e tentativa de manusear as dependencias manualmente não funciona para esse cenário.

Para isso, é necessário disponibilizar a imagem via ECR para que possa ser usada como base para um lambda

### Passo a passo

#### 1. Instale o cli do aws [instruções aqui](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) e logue usando access key e secret key

```
aws configure
```

#### 2. Garanta que o docker está rodando sem precisar de sudo

#### 3. Crie o repositorio no ECR

```
aws ecr create-repository \
  --repository-name tech-challenge \
  --region us-east-1

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="$ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com"

aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin "$ECR_URI"
```

#### 4. Construa a imagem docker localmente

```
docker buildx build --platform linux/arm64 --no-cache --provenance=false --output=type=docker -t finance-fetcher:v1 .
```

#### 5. Disponibilize a imagem no ECR

```
docker tag finance-fetcher:local "$ECR_URI/tech-challenge:v1"
docker push "$ECR_URI/tech-challenge:v1"
```

#### 6. Crie o lambda usando a imagem disponibilizada

No painel da AWS, crie uma lambda function. Na opção para "como criar", selecione "Container Image".

Clique em "Browse Images", e selecione o repositorio tech-challenge. Depois selecione a imagem disponibilizada.

#### 7. Configure o timeout da lambda

Depois de criada, aumente o timeout da lambda de 3 segundos (default) para 30 segundos.

#### 8. Garanta que a role que está executando a function tem permissão para escrever no bucket usado no s3

#### 9. Gatilho de Execução
A ingestão dos dados é executada de forma automática por meio de uma regra do **Amazon EventBridge (CloudWatch Events)**.

- **Nome da regra:** `execucao_diaria_ingestao`
- **Serviço:** Amazon EventBridge
- **Estado:** ENABLED
- **Descrição:** Trigger diário da ETL
- **Expressão CRON:** `cron(0 12 ? * MON-FRI *)`

Essa regra agenda a execução do pipeline de **segunda a sexta-feira, às 12h (UTC)**, alinhando a ingestão aos dias úteis do mercado financeiro.  
Ao ser acionada, a regra dispara a **Lambda de ingestão**, que inicia o fluxo de extração dos dados.

## ETL com AWS Glue
O AWS Glue é responsável pelo processamento e enriquecimento dos dados financeiros ingeridos na camada raw do S3. O job foi implementado em **PySpark**, utilizando **AWS Glue versão 5.0 (Spark 3.5, Python 3)**, permitindo processamento distribuído e escalável. O job é executado sob demanda e iniciado por uma função Lambda, que atua apenas como orquestradora do fluxo.

### Configuração do Job

- **Tipo:** Spark
- **Linguagem:** Python 3
- **Worker type:** G.1X (4 vCPUs, 16 GB RAM)
- **Número de workers:** 2
- **Modo de execução:** Script (Glue Job via código)

### Leitura dos Dados

O job realiza a leitura dos dados brutos diretamente do S3:

- **Origem:** tech-challenge-raw
- **Formato:** Parquet
- **Granularidade:** diária
- **Escopo:** índice Bovespa (^BVSP)
- Transformações Aplicadas (ETL)

### Processamento

#### Limpeza e Padronização

- Remoção de colunas técnicas ou desnecessárias
- Renomeação de colunas para nomes mais descritivos (ex.: Open → preco_abertura, Close → preco_fechamento)

#### Cálculos Temporais (Baseados em Data)

- Utilizando janelas de tempo (Window Functions), são calculadas métricas como:
  - Médias móveis de 3, 5 e 7 dias
  - Volume médio por período
  - Valores mínimo e máximo em janelas móveis
  - Diferença de preço entre períodos (3d, 5d, 7d)
  - Amplitude diária e retorno percentual diário

#### Agregações Mensais (Camada Enriched)

Além da tabela diária refinada, o job gera uma tabela agregada mensal, contendo:

- Preço médio de fechamento
- Máximo e mínimo mensal
- Volume total negociado
- Volatilidade média do mês

### Escrita dos Dados

Após as transformações, os dados são persistidos no S3:

#### **Camada Refined**

- **Bucket:** tech-challenge-refined
- **Formato:** Parquet
- **Particionamento:** ticker e data_pregao
- **Conteúdo:** dados diários tratados e enriquecidos

#### Camada Enriched

- **Pasta:** enriched/analises_mensais/
- **Particionamento:** ano e mes
- **Conteúdo:** métricas agregadas mensais

#### Catalogação no Glue Data Catalog

O job realiza automaticamente:

- Criação do banco de dados (_workspace_db_)
- Registro ou atualização das tabelas: _tb_bvsp_refined_ e _tb_bvsp_mensal_
- Sincronização das partições via MSCK REPAIR TABLE

Com isso, os dados ficam imediatamente disponíveis para consulta no AWS Athena, utilizando SQL padrão.

#### Resultado Final

Ao término da execução, o pipeline entrega:

- Dados diários refinados e particionados
- Dados agregados mensais para análise
- Tabelas catalogadas e prontas para consulta analítica.

## Disparo do Job AWS Glue via S3

O processamento dos dados no **AWS Glue** é acionado automaticamente a partir de eventos no **Amazon S3**.

### Gatilho por Evento S3

O bucket responsável pela ingestão dos dados brutos foi configurado para disparar uma função **Lambda** sempre que novos arquivos forem gravados na camada *raw*.

- **Bucket:** `tech-challenge-ingestion`
- **Evento:** `s3:ObjectCreated:Put`
- **Prefixo monitorado:** `raw/`
- **Serviço acionador:** Amazon S3

Essa configuração garante que o processamento seja iniciado **somente quando novos dados estiverem disponíveis**, evitando execuções desnecessárias do job de ETL.

### Lambda Orquestradora do Glue

A função Lambda associada ao evento S3 tem como única responsabilidade **iniciar o job do AWS Glue**, sem realizar transformações de dados:

```
import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    job_name = "tech-challenge-etl-diario"
    
    try:
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        
        print(f"Job do Glue iniciado com sucesso! RunId: {job_run_id}")
        return {
            'statusCode': 200,
            'body': f"Job {job_name} iniciado com ID: {job_run_id}"
        }
        
    except Exception as e:
        print(f"Erro ao iniciar o Job: {str(e)}")
        raise e

```

Ao receber o evento de criação de arquivos no S3, a Lambda executa a chamada:

- **Job Glue:** `tech-challenge-etl-diario`
- **Ação:** `start_job_run`

## Consulta dos Dados com AWS Athena

Os dados processados pelo AWS Glue são disponibilizados para análise por meio do **AWS Athena**, permitindo consultas utilizando **SQL padrão** diretamente sobre os arquivos armazenados no S3.

### Integração com Glue Data Catalog

As tabelas geradas pelo job do Glue são automaticamente registradas no **Glue Data Catalog**, possibilitando que o Athena reconheça os esquemas e partições sem necessidade de configuração manual.

As principais tabelas disponíveis para consulta são:

- **`tb_bvsp_refined`**: dados diários tratados e enriquecidos, particionados por `ticker` e `data_pregao`
- **`tb_bvsp_mensal`**: dados agregados mensais, particionados por `ano` e `mes`

### Resultados das Consultas

Os resultados das consultas executadas no Athena são armazenados no bucket:

- **`tech-challenge-refined/query-results/`**

Esse padrão permite auditoria, reprocessamento e reutilização dos resultados das análises.

### Exemplos de Consultas no Athena

Abaixo estão alguns exemplos de consultas SQL que podem ser executadas no AWS Athena para validação e análise dos dados processados.

#### 1. Consulta básica – dados diários refinados

```sql
SELECT *
FROM workspace_db.tb_bvsp_refined
ORDER BY data_pregao DESC
LIMIT 10;
```

#### 2. Dias com maior volatilidade

```sql
SELECT
    data_pregao,
    amplitude_diaria
FROM workspace_db.tb_bvsp_refined
ORDER BY amplitude_diaria DESC
LIMIT 10;
```

#### 3. Meses com maior volume negociado

```sql
SELECT
    ano,
    mes,
    volume_total_mes
FROM workspace_db.tb_bvsp_mensal
ORDER BY volume_total_mes DESC
LIMIT 5;
```
