# instruction for boto3 https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
import logging
import boto3
from botocore.exceptions import ClientError
import os
import datetime
import json
import ast
import schedule
import time

path = '/Users/oksanasyceva/Downloads/'
file_name = 'vectors_news_habr.json'
path_and_file_name= path+file_name

file_for_test = '/Users/oksanasyceva/Downloads/new_file.json'

bucket = 'kmtestbucket'
updated_bucket = 'updatedkmtestbucket'

session = boto3.session.Session()
s3 = session.client(
    aws_access_key_id='YCAJEH8YSbeEcRFd8tyEPCZ_k',
    aws_secret_access_key='YCPpiC8l4PKeyGfvZIl-ZwHFtozXs8ibqbKNBCkT',
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)

#создание бакета
#оно должно быть уникально для всего яндекса клауда
# s3.create_bucket(Bucket='bucketppppppname')


def upload_file(path_and_file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(path_and_file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3.upload_file(path_and_file_name, bucket, object_name)
        print('File was uploaded')
    except ClientError as e:
        logging.error(e)
        return False
    return path_and_file_name, bucket

# Получить список объектов в бакете
def get_object_list(bucket):
    objects = []
    try:
        for key in s3.list_objects(Bucket=bucket)['Contents']:
            objects.append(key['Key'])
    except Exception as e: 
        print(e)
        return False
    return objects

# Удалить несколько объектов
# НЕ возвращает ошибку, если попытаться удалить несуществующий файл
def delete_object(object_name, bucket):
    try:
        forDeletion = [{'Key':object_name}]
        response = s3.delete_objects(Bucket=bucket, Delete={'Objects': forDeletion})
        print('File was deleted')
    except Exception as e: 
        print(e)
        return False
    return object_name
#delete_object(bucket,'folder1/')

# Получить объект (покажет данные внутри объекта)
def get_object(object_name, bucket):
    object_data = ''
    try:
        get_object_response = s3.get_object(Bucket=bucket,Key=object_name)
        object_data = get_object_response['Body'].read()
        print('File was downloaded')
    except Exception as e: 
        print(e)
        return False
    return object_data
#get_object(bucket,'vectors_news_habr.json')

# Изменить объект
def change_object(file, file_name):
    try:
        changed_file = {"test" : 'try_new_title'}
        new_file_name = str(datetime.datetime.now())+ ' ' +file_name
        print('Changes have been made')
    except Exception as e: 
        print(e)
        return False
    return changed_file, new_file_name

def upload_data(bucket, key, body,storageclass='STANDARD'):
    """bucket - бакет, куда грузим
    key - как файл будет называться
    body - что за строку грузим
    StorageClass - класс хранения:
                Стандартное хранилище — STANDARD.
                Холодное хранилище — COLD, STANDARD_IA или NEARLINE (последние два — только при загрузке объектов в бакет).
                Ледяное хранилище — ICE, GLACIER (последний — только при загрузке объектов в бакет).
    """
    try:
        new_body = json.dumps(body, indent=2).encode('utf-8')
        s3.put_object(Bucket=bucket, Key=key, Body=new_body, StorageClass=storageclass)
        print('Data was uploaded')
    except Exception as e: 
        print(e)
        return False

def infinity_request():
    i = 0
    while True:
        
        if i<=3:
            print('START')
            print(datetime.datetime.now(),'pre step')

            uploaded_path_and_file_name, uploaded_bucket = upload_file(path_and_file_name, bucket)
            print(datetime.datetime.now(), 'after upload time')

            downloaded_file_data = get_object(file_name, uploaded_bucket)
            #из bytes превращаем в словарь, чтобы дальше с ним работать
            normal_downloaded_file_data = ast.literal_eval(downloaded_file_data.decode('utf-8')) 
            print(datetime.datetime.now(), 'after download time')

            changed_file, new_file_name = change_object(normal_downloaded_file_data, file_name)
            print(datetime.datetime.now(), 'after change time')

            new_file_in_yandex = upload_data(updated_bucket, key=new_file_name, body = changed_file)
            print(datetime.datetime.now(), 'after new upload time')
            print('END')
            i+=1

# infinity_request()
schedule.every(10).minutes.do(infinity_request)
# schedule.every().day.at("10:30").do(job)

# # нужно иметь свой цикл для запуска планировщика с периодом в 1 секунду:
while True:
    schedule.run_pending()
    time.sleep(1)

# schedule.CancelJob
# scheduler.cancel(event)
