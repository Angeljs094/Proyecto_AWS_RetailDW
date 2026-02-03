# Importar librerías esenciales de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from datetime import datetime

# 1. Iniciar la Sesión de Spark (El punto de entrada)
# 'appName' ayuda a identificar el job en el monitor de YARN
spark = SparkSession.builder \
    .appName("ArticuloAgregacionJob") \
    .getOrCreate()

# Fecha dinámica para el archivo de entrada
# 2. Usamos S3 como sistema de archivos, no el disco local
today_str = datetime.today().strftime("%d-%m-%y")
input_path = f"s3://dmc-aws-project-atjs1/raw/erp/articulo/articulo_{today_str}.csv"
output_path = "s3://dmc-aws-project-atjs1/trusted/articulo/"

print(f"Leyendo datos desde: {input_path}")

# 3. Lectura de Datos (Extract)
# Spark infiere el esquema (schema inference) pero indicamos que tiene encabezado
articulo = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

# 4. Transformaciones
# Eliminar duplicados por art_id
articulo = articulo.dropDuplicates(["art_id"])

# Eliminar filas con nom o cat vacíos/nulos
articulo = articulo.dropna(subset=["nom", "cat"])

# Normalizar texto en nom y cat (quitar espacios extra)
articulo = articulo.withColumn("nom", trim(col("nom"))) \
                   .withColumn("cat", trim(col("cat")))

# Validar precios y stock: reemplazar valores inválidos
articulo = articulo.withColumn("prec", when(col("prec") <= 0, None).otherwise(col("prec"))) \
                   .withColumn("stock", when(col("stock") < 0, None).otherwise(col("stock")))

# Normalizar fechas: si fmod es nulo, usar fcre
articulo = articulo.withColumn("fmod", when(col("fmod").isNull(), col("fcre")).otherwise(col("fmod")))

# 5. Escritura de Resultados (Load)
# Escribimos en formato Parquet (columnar, comprimido, ideal para Big Data)
# mode("overwrite") sobrescribe si ya existe la carpeta
articulo.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Procesamiento completado. Resultados en: {output_path}")

# Detener la sesión para liberar recursos
spark.stop()
