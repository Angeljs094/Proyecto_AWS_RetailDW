# -----------------------------------------------------------------------------
# AWS Glue Python Shell Job - Extracción Parametrizada desde PostgreSQL a S3
# -----------------------------------------------------------------------------
#
# Descripción:
# Este script, diseñado para AWS Glue Python Shell, extrae datos de una tabla
# PostgreSQL usando Pandas, la guarda localmente como CSV y la sube a S3 con Boto3.
# El esquema será inferido posteriormente por un AWS Glue Crawler.
#
# Parámetros del Job:
# --secret_name:                  Nombre del secreto en AWS Secrets Manager.
# --source_dbtable:               Tabla de origen en formato 'esquema.tabla'.
# --s3_target_bucket:             Bucket S3 de destino.
# --s3_target_path:               Ruta dentro del bucket S3 (sin incluir el nombre del archivo).
# --file_prefix:                  Prefijo para el nombre del archivo (ejemplo: tienda_)
# -----------------------------------------------------------------------------

import sys
import boto3
import json
import pandas as pd
import psycopg2
import argparse
import os
from datetime import datetime
from botocore.exceptions import ClientError

def get_secret(secret_name):
    """Recupera un secreto de AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])

def main():
    # 1. OBTENCIÓN DE PARÁMETROS DEL JOB
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret_name", required=True)
    parser.add_argument("--source_dbtable", required=True)
    parser.add_argument("--s3_target_bucket", required=True)
    parser.add_argument("--s3_target_path", required=True)
    parser.add_argument("--file_prefix", required=True)  # nuevo parámetro
    args, _ = parser.parse_known_args()

    db_credentials = get_secret(args.secret_name)

    # 2. EXTRACCIÓN DESDE POSTGRESQL
    conn = psycopg2.connect(
        host=db_credentials['host'],
        port=db_credentials['port'],
        dbname=db_credentials['dbname'],
        user=db_credentials['username'],
        password=db_credentials['password']
    )
    df_new = pd.read_sql_query(f"SELECT * FROM {args.source_dbtable}", conn)
    conn.close()

    if df_new.empty:
        print("No se encontraron datos nuevos.")
        return

    # 3. DEFINICIÓN DE NOMBRE DE ARCHIVO CON PREFIJO + FECHA
    today_str = datetime.today().strftime("%d-%m-%y")
    file_name = f"{args.file_prefix}{today_str}.csv"
    s3_client = boto3.client('s3')
    s3_file_key = f"{args.s3_target_path}/{file_name}"

    # 4. MERGE CON ARCHIVO EXISTENTE (Upsert usando t_id)
    try:
        existing_obj = s3_client.get_object(Bucket=args.s3_target_bucket, Key=s3_file_key)
        df_existing = pd.read_csv(existing_obj['Body'])
        print(f"Archivo existente encontrado con {len(df_existing)} registros.")

        # Concatenar y aplicar "upsert"
        df_all = pd.concat([df_existing, df_new], ignore_index=True)

        # Agrupar por clave primaria (t_id) y quedarnos con el último
        if 't_id' in df_all.columns:
            df_final = df_all.sort_values('t_id').drop_duplicates(subset=['t_id'], keep='last')
        else:
            df_final = df_all.drop_duplicates(keep='last')

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("No existe archivo previo para hoy. Usando solo los datos nuevos.")
            df_final = df_new
        else:
            raise

    # 5. GUARDAR RESULTADO EN S3
    local_csv_path = "/tmp/output.csv"
    df_final.to_csv(local_csv_path, index=False)

    s3_client.upload_file(local_csv_path, args.s3_target_bucket, s3_file_key)
    os.remove(local_csv_path)

    print(f"Archivo final con {len(df_final)} registros guardado en s3://{args.s3_target_bucket}/{s3_file_key}")

if __name__ == "__main__":
    main()
