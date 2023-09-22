import os
import json
import boto3
import csv
from io import StringIO
from datetime import datetime

# Configuración de cliente para interactuar con AWS
s3_client = boto3.client('s3')
secrets_manager_client = boto3.client('secretsmanager')
dynamodb_client = boto3.client('dynamodb')
ses_client = boto3.client('ses')

def get_secret(secret_name):
    try:
        secret_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_response['SecretString'])
    except Exception as e:
        print(f"Error al obtener el secreto: {str(e)}")
        return None

def download_csv_from_s3(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error al descargar el archivo CSV desde S3: {str(e)}")
        return None

def process_csv(csv_data, table_name):
    csv_reader = csv.reader(StringIO(csv_data), delimiter=';')
    total_registros = 0
    registros_procesados = 0

    for row in csv_reader:
        total_registros += 1
        id, dsccls, tpotrj, prefijo, banco, clscta, indpre, indblq, indcls, indunf = row

        # Solo por prueba en log imprimo registros
        print(f'id: {id}, dsccls: {dsccls}, tpotrj: {tpotrj}, prefijo: {prefijo}, banco: {banco}, clscta: {clscta}, indpre: {indpre}, indblq: {indblq}, indcls: {indcls}, indunf: {indunf}')

        try:
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    'id': {'S': id},
                    'dsccls': {'S': dsccls},
                    'tpotrj': {'S': tpotrj},
                    'prefijo': {'S': prefijo},
                    'banco': {'S': banco},
                    'clscta': {'S': clscta},
                    'indpre': {'S': indpre},
                    'indblq': {'S': indblq},
                    'indcls': {'S': indcls},
                    'indunf': {'S': indunf}
                }
            )
            registros_procesados += 1
        except Exception as e:
            print(f'Error al procesar el registro: {str(e)}')

    print(f'Registros totales: {total_registros}')
    print(f'Registros procesados: {registros_procesados}')

def send_email_notification(sender_email, recipient_email, subject, message):
    try:
        ses_client.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [recipient_email],
            },
            Message={
                'Subject': {
                    'Data': subject,
                },
                'Body': {
                    'Text': {
                        'Data': message,
                    },
                },
            },
        )
    except Exception as e:
        print(f"Error al enviar el correo electrónico: {str(e)}")

def lambda_handler(event, context):
    # Nombre del bucket y la clave (nombre) del archivo que desencadenó la Lambda
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Obtener secretos de AWS Secrets Manager
    secret_name = "RimegaRDSSecret"  # Nombre del secreto en Secrets Manager
    secret_data = get_secret(secret_name)

    if secret_data is not None:
        # Solo a Modo de Prueba imprimo valores obtenidos del secreto
        print(f'Host: {secret_data["host"]}')
        print(f'Port: {secret_data["port"]}')
        print(f'User: {secret_data["user"]}')
        print(f'Password: {secret_data["password"]}')

        # Descargar y procesar el archivo CSV desde S3
        csv_data = download_csv_from_s3(bucket, key)
        if csv_data is not None:
            # Nombre de la tabla DynamoDB
            table_name = "clasesPlastico"

            # Procesar el archivo CSV
            process_csv(csv_data, table_name)

            # Enviar un correo electrónico de notificación
            sender_email = "test92@gmail.com"  # Dirección del remitente
            recipient_email = "Test@QWERTY.com.co"  # Dirección de correo del destinatario
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subject = f"Procesamiento completado: {key} ({current_datetime})"
            message = f"El archivo {key} se ha procesado correctamente el {current_datetime}."
            send_email_notification(sender_email, recipient_email, subject, message)

    return {
        'statusCode': 200,
        'body': json.dumps('Procesamiento completado con éxito.')
    }
