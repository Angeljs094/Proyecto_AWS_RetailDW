# Importar librerías esenciales de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from datetime import datetime

# 1. Iniciar la Sesión de Spark (El punto de entrada)
# 'appName' ayuda a identificar el job en el monitor de YARN
spark = SparkSession.builder \
    .appName("TiendaAgregacionJob") \
    .getOrCreate()

# Fecha dinámica para el archivo de entrada
# 2. Usamos S3 como sistema de archivos, no el disco local
today_str = datetime.today().strftime("%d-%m-%y")
input_path = f"s3://dmc-aws-project-atjs1/raw/erp/tienda/tienda_{today_str}.csv"
output_path = "s3://dmc-aws-project-atjs1/trusted/tienda/"

print(f"Leyendo datos desde: {input_path}")

# 3. Lectura de Datos (Extract)
# Spark infiere el esquema (schema inference) pero indicamos que tiene encabezado
df_new = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

df_new.printSchema()

# Transformaciones
df_new = df_new.dropDuplicates(["t_id"]) \
               .dropna(subset=["nom", "ubic"]) \
               .withColumn("ubic", trim(col("ubic"))) \
               .withColumn("nom", trim(col("nom")))

# 5. Escritura de Resultados (Load)
# Escribimos en formato Parquet (columnar, comprimido, ideal para Big Data)
# mode("overwrite") sobrescribe si ya existe la carpeta
df_new.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Procesamiento completado. Resultados en: {output_path}")

# Detener la sesión para liberar recursos
spark.stop()

