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

Observação: todos os arquivos abaixo são utilizados na imagem Docker com exceção do glue_job.py, que corresponde exclusivamente ao script executado no AWS Glue para aplicação das transformações de ETL e gravação dos dados refinados.

## Lambda para Ingestão de Dados

Na pasta **ingest/** estão os artefatos necessários para a criação da imagem Docker utilizada pelo AWS Lambda responsável pela ingestão dos dados (exceto o script *glue_job*, usado para o ETL na AWS Glue).

Essa abordagem é necessária devido às limitações do Lambda tradicional (ZIP), especialmente quando se trabalha com bibliotecas como numpy e pyarrow.

## data_handler.py

no **data_handler.py** estao os métodos para criar o arquivo em formato parquet no s3. O bucket e a pasta "raw" do S3 estão descritos como variáveis no começo do arquivo.

## docker

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
