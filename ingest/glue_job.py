import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.window import Window
import logging

# configura o Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# parâmetros de execução do Glue (se houver)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# configura o SparkContext e o GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") # Permite registrar sem sobreescrever os arquivos

# inicializando o job Glue
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leitura dos dados
path_raw = "s3://tech-challenge-ingestion/raw/^BVSP/"
df_raw = spark.read.parquet(path_raw)

# 2. Limpar e renomear colunas
df = df_raw.drop("Price", "index", "__index_level_0__") \
    .withColumnRenamed("dt", "data_pregao") \
    .withColumnRenamed("Open", "preco_abertura") \
    .withColumnRenamed("Close", "preco_fechamento") \
    .withColumnRenamed("High", "maior_preco") \
    .withColumnRenamed("Low", "menor_preco") \
    .withColumnRenamed("Volume", "volume") 

# 3. Janelas de tempo
window_3d = Window.orderBy("data_pregao").rowsBetween(-3, -1)
window_5d = Window.orderBy("data_pregao").rowsBetween(-5, -1)
window_7d = Window.orderBy("data_pregao").rowsBetween(-7, -1)

# 4. Cálculos no Tempo:
df = df.withColumn("media_movel_3d", F.avg("preco_fechamento").over(window_3d)) \
        .withColumn("media_movel_5d", F.avg("preco_fechamento").over(window_5d)) \
        .withColumn("media_movel_7d", F.avg("preco_fechamento").over(window_7d)) \
        .withColumn("volume_medio_3d", F.avg("volume").over(window_3d)) \
        .withColumn("volume_medio_5d", F.avg("volume").over(window_5d)) \
        .withColumn("volume_medio_7d", F.avg("volume").over(window_7d)) \
        .withColumn("valor_minimo_7_dias", F.min("preco_fechamento").over(window_7d)) \
        .withColumn("valor_maximo_7_dias", F.max("preco_fechamento").over(window_7d))

# Oscilações da bolsa
df = df.withColumn("diff_3d", F.col("preco_fechamento") - F.lag("preco_fechamento", 3).over(Window.orderBy("data_pregao"))) \
        .withColumn("diff_5d", F.col("preco_fechamento") - F.lag("preco_fechamento", 5).over(Window.orderBy("data_pregao"))) \
        .withColumn("diff_7d", F.col("preco_fechamento") - F.lag("preco_fechamento", 7).over(Window.orderBy("data_pregao"))) \
        .withColumn("amplitude_diaria", F.col("maior_preco") - F.col("menor_preco")) \
        .withColumn("retorno_diario_perc", ((F.col("preco_fechamento") - F.col("preco_abertura")) / F.col("preco_abertura"))*100)

# Agregações - Level Enriched
df_mensal = df.withColumn("ano", F.year("data_pregao")) \
              .withColumn("mes", F.month("data_pregao")) \
              .groupBy("ano", "mes") \
              .agg(
                F.avg("preco_fechamento").alias("media_fechamento"),
                F.max("maior_preco").alias("max_preco_mes"),
                F.min("menor_preco").alias("min_preco_mes"),
                F.sum("volume").alias("volume_total_mes"),
                F.avg("amplitude_diaria").alias("volatilidade_media_mes")
            )
            
# Forçando conversão das variaveis numéricas
colunas_numericas = [
    "preco_abertura", "preco_fechamento", "maior_preco", "menor_preco", "volume",
    "media_movel_3d", "media_movel_5d", "media_movel_7d",
    "volume_medio_3d", "volume_medio_5d", "volume_medio_7d",
    "valor_minimo_7_dias", "valor_maximo_7_dias",
    "diff_3d", "diff_5d", "diff_7d", 
    "amplitude_diaria", "retorno_diario_perc"
]

# Aplica o cast no DataFrame principal
for col_name in colunas_numericas:
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))

# Aplica o cast no DataFrame mensal (que tem nomes de colunas diferentes)
colunas_mensais = ["media_fechamento", "max_preco_mes", "min_preco_mes", "volume_total_mes", "volatilidade_media_mes"]
for col_name in colunas_mensais:
    if col_name in df_mensal.columns:
        df_mensal = df_mensal.withColumn(col_name, F.col(col_name).cast("double"))

# Salvar Parquet Particionado na Camada Refined
path_refined = "s3://tech-challenge-refined/refined/"

df_save = df.withColumn("data_pregao", F.col("data_pregao").cast("string")) \
            .withColumn("ticker", F.lit("BVSP"))
 
df_save.write \
    .mode("overwrite") \
    .partitionBy("ticker", "data_pregao") \
    .parquet(path_refined)

logger.info("Tabela Refined particionada por data e ticker salva com sucesso")

# Salvar Parquet Completo na Camada Enriched
path_enriched_mensal = "s3://tech-challenge-refined/enriched/analises_mensais/"
df_mensal.write \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .parquet(path_enriched_mensal)

logger.info("Tabela Agregada salva com sucesso!")

# Registrando tabelas no Athena
glue_client = boto3.client("glue")
database_name = "workspace_db"

try:
    glue_client.get_database(Name=database_name)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(DatabaseInput={'Name': database_name})

def registrar_no_catalog(table_name, s3_location, columns, partition_keys=[]):
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            }
        },
        'PartitionKeys': partition_keys,
        'TableType': 'EXTERNAL_TABLE'
    }
    
    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    
    if partition_keys:
        spark.sql(f"MSCK REPAIR TABLE {database_name}.{table_name}")

# Defininindo colunas
cols_refined = [
    {"Name": "preco_abertura", "Type": "double"},
    {"Name": "preco_fechamento", "Type": "double"},
    {"Name": "maior_preco", "Type": "double"},
    {"Name": "menor_preco", "Type": "double"},
    {"Name": "volume", "Type": "double"},
    {"Name": "media_movel_3d", "Type": "double"},
    {"Name": "media_movel_5d", "Type": "double"},
    {"Name": "media_movel_7d", "Type": "double"},
    {"Name": "volume_medio_3d", "Type": "double"},
    {"Name": "volume_medio_5d", "Type": "double"},
    {"Name": "volume_medio_7d", "Type": "double"},
    {"Name": "diff_3d", "Type": "double"},
    {"Name": "diff_5d", "Type": "double"},
    {"Name": "diff_7d", "Type": "double"},
    {"Name": "amplitude_diaria", "Type": "double"},
    {"Name": "retorno_diario_perc", "Type": "double"}
]

cols_mensal = [
    {"Name": "media_fechamento", "Type": "double"},
    {"Name": "max_preco_mes", "Type": "double"},
    {"Name": "min_preco_mes", "Type": "double"},
    {"Name": "volume_total_mes", "Type": "double"},
    {"Name": "volatilidade_media_mes", "Type": "double"}
]

# Chamadas de Registro
registrar_no_catalog("tb_bvsp_refined", path_refined, cols_refined, 
                     [{"Name": "ticker", "Type": "string"}, {"Name": "data_pregao", "Type": "string"}])
registrar_no_catalog("tb_bvsp_mensal", path_enriched_mensal, cols_mensal, 
                     [{"Name": "ano", "Type": "int"}, {"Name": "mes", "Type": "int"}])

logger.info("Tabelas registradas e sincronizadas no Athena.")

logger.info("Job executado e finalizado com sucesso")

job.commit()