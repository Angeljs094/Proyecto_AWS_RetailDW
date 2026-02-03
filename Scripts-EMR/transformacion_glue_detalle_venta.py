# Importar librerías esenciales de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from datetime import datetime

# 1. Iniciar la Sesión de Spark (El punto de entrada)
# 'appName' ayuda a identificar el job en el monitor de YARN
spark = SparkSession.builder \
    .appName("DetalleVentaAgregacionJob") \
    .getOrCreate()

# Fecha dinámica para el archivo de entrada
# 2. Usamos S3 como sistema de archivos, no el disco local
today_str = datetime.today().strftime("%d-%m-%y")
input_path = f"s3://dmc-aws-project-atjs1/raw/erp/detalle_venta/detalle_venta_{today_str}.csv"
output_path = "s3://dmc-aws-project-atjs1/trusted/detalle_venta/"

print(f"Leyendo datos desde: {input_path}")

# 3. Lectura de Datos (Extract)
# Spark infiere el esquema (schema inference) pero indicamos que tiene encabezado
detalle = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

# 4. Transformaciones
# Eliminar duplicados por det_id
detalle = detalle.dropDuplicates(["det_id"])

# Eliminar filas con claves vacías (v_id o art_id)
detalle = detalle.dropna(subset=["v_id", "art_id"])

# Normalizar texto en claves
detalle = detalle.withColumn("det_id", trim(col("det_id"))) \
                 .withColumn("v_id", trim(col("v_id"))) \
                 .withColumn("art_id", trim(col("art_id"))) \
                 .withColumn("m_und", trim(col("m_und")))

# Validar cantidad y subtotal: reemplazar valores inválidos
detalle = detalle.withColumn("cant", when(col("cant") < 0, None).otherwise(col("cant"))) \
                 .withColumn("sub_tot", when(col("sub_tot") < 0, None).otherwise(col("sub_tot")))


# Escritura de Resultados (Load)
# Escribimos en formato Parquet (columnar, comprimido, ideal para Big Data)
# mode("overwrite") sobrescribe si ya existe la carpeta
detalle.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Procesamiento completado. Resultados en: {output_path}")

# Detener la sesión para liberar recursos
spark.stop()
