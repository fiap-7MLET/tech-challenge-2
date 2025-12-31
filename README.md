# tech-challenge-2

## Lambda para ingest

Na pasta **ingest** estão os artefatos necessários para criar a imagem a ser usada no Lambda

## data_handler.py

no **data_handler.py** estao os métodos para criar o arquivo em formato parquet no s3. O bucket e a pasta "raw" do S3 estão descritos como variáveis no começo do arquivo.

## docker

Devido a restrições da estrutura do lambda e a utilização de bibliotecas como numpy e pyarrow, é preciso criar uma imagem base com as dependencias necessárias e usar essa imagem para o lambda. o upload de zip e tentativa de manusear as dependencias manualmente não funciona para esse cenário.

Para isso, é necessário disponibilizar a imagem via ECR para que possa ser usada como base para um lambda

### passo a passo

#### 1. Instale o cli do aws [instruções aqui](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) e logue usando access key e secret key

```
aws configure
```

#### 2. Garanta que o docker está rodando sem precisar de sudo

#### 3. Crie o repositorio no ECR (usei o nome "tech-challenge")

```
aws ecr create-repository \
  --repository-name tech-challenge \
  --region sa-east-1

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="$ACCOUNT_ID.dkr.ecr.sa-east-1.amazonaws.com"

aws ecr get-login-password --region sa-east-1 \
  | docker login --username AWS --password-stdin "$ECR_URI"
```

#### 4. construa a imagem docker localmente

```
docker build -t finance-fetcher:local .
```

#### 5. Disponibilize a imagem no ECR

```
docker tag finance-fetcher:local "$ECR_URI/tech-challenge:0.0.1"
docker push "$ECR_URI/tech-challenge:0.0.1"
```

#### 6. Crie o lambda usando a imagem disponibilizada

no painel da AWS, crie uma lambda function. Na opção para "como criar", selecione "Container Image"

Clique em "Browse Images", e selecione o repositorio tech-challenge. Depois selecione a imagem disponibilizada

#### 7. Configure o timeout da lambda

Depois de criada, aumente o timeout da lambda de 3 segundos (default) para algo maior, devido a busca no yfinance e persistencia no s3. 30 segundos é mais que o suficiente

#### 8. Garanta que a role que está executando a function tem permissão para escrever no bucket usado no s3

Sem isso, a execução dará erro na hora de persistir o parquet