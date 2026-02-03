# Importar librerías esenciales de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from datetime import datetime

# 1. Iniciar la Sesión de Spark (El punto de entrada)
# 'appName' ayuda a identificar el job en el monitor de YARN
spark = SparkSession.builder \
    .appName("VentasAgregacionJob") \
    .getOrCreate()

# Fecha dinámica para el archivo de entrada
# 2. Usamos S3 como sistema de archivos, no el disco local
today_str = datetime.today().strftime("%d-%m-%y")
input_path = f"s3://dmc-aws-project-atjs1/raw/erp/venta/ventas_{today_str}.csv"
output_path = "s3://dmc-aws-project-atjs1/trusted/ventas/"

print(f"Leyendo datos desde: {input_path}")

# 3. Lectura de Datos (Extract)
# Spark infiere el esquema (schema inference) pero indicamos que tiene encabezado
ventas = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

# 4. Transformaciones
# Eliminar duplicados por v_id
ventas = ventas.dropDuplicates(["v_id"])

# Eliminar filas con claves vacías (t_id o v_dt)
ventas = ventas.dropna(subset=["t_id", "v_dt"])

# Normalizar texto en claves (quitar espacios extra)
ventas = ventas.withColumn("v_id", trim(col("v_id"))) \
               .withColumn("t_id", trim(col("t_id")))

# Validar totales: reemplazar valores inválidos
ventas = ventas.withColumn("total", when(col("total") < 0, None).otherwise(col("total")))

# Opcional: renombrar v_dt a fecha para consistencia
ventas = ventas.withColumnRenamed("v_dt", "fecha")

# Escritura de Resultados (Load)
# Escribimos en formato Parquet (columnar, comprimido, ideal para Big Data)
# mode("overwrite") sobrescribe si ya existe la carpeta
ventas.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Procesamiento completado. Resultados en: {output_path}")

# Detener la sesión para liberar recursos
spark.stop()


