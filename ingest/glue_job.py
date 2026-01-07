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

# inicializando o job Glue
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leitura dos dados
path_raw = "s3://tech-challenge-ingestion/raw/^BVSP/"
df_raw = spark.read.parquet(path_raw)

# 2. Limpar e renomear colunas
df = df_raw.drop("Price", "index") \
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
        .withColumn("retorno_diario_perc", (F.col("preco_fechamento") - F.col("preco_abertura")) / F.col("preco_abertura"))

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

# Salvar Parquet Particionado na Camada Refined
path_refined = "s3://tech-challenge-refined/refined/"

df_save = df.withColumn("data_pregao", F.col("data_pregao").cast("string")) \
            .withColumn("ticker", F.lit("^BVSP"))
            
df_save.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("ticker", "data_pregao") \
    .option("path", path_refined) \
    .option("external", "true") \
    .saveAsTable("default.tb_bvsp_refined")
    
spark.sql("MSCK REPAIR TABLE default.tb_bvsp_refined")
logger.info("Tabela Refined particionada por data e ticker salva com sucesso")

# Salvar Parquet Completo na Camada Enriched 
# Salvar df_mensal -  Tabela agregada
path_enriched_mensal = "s3://tech-challenge-refined/enriched/analises_mensais/"
df_mensal.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("ano", "mes") \
    .option("path", path_enriched_mensal) \
    .option("external", "true") \
    .saveAsTable("default.tb_bvsp_mensal_enriched")
    
spark.sql("MSCK REPAIR TABLE default.tb_bvsp_mensal_enriched")

# Histórico Completo
path_full_history = "s3://tech-challenge-refined/enriched/historico_completo/"
df_save.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", path_full_history) \
    .option("external", "true") \
    .saveAsTable("default.tb_bvsp_historico_full")
    
spark.sql("REFRESH TABLE default.tb_bvsp_historico_full")

logger.info("Tabelas Agregadas e Histórico Completo salvas com sucesso!")

logger.info("Job executado e finalizado com sucesso")

job.commit()