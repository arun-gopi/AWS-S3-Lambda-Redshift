import boto3
from zipfile import ZipFile

import os
import json
import uuid
import psycopg2
from datetime import datetime

myuuid = str(uuid.uuid4()).replace('-','')

IAM_ROLE = os.environ.get('IAM_ROLE')
Access_key = os.environ.get('AWS_Access_key') 
Access_Secrete = os.environ.get('AWS_Access_Secrete') 
dbname = os.getenv('dbname')
host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
tablename = os.getenv('tablename')
lambda_client = boto3.client('lambda')


def _get_base_copy_cmd(table: str, path: str) -> str:
    """ Base of any Redshift Copy Command """
    return """
            COPY {table}
            FROM '{path}'
            IAM_ROLE '{iam_role}'
            """.format(table=table, path=path,
                       iam_role=IAM_ROLE)


def _get_csv_copy_cmd(table: str, path: str) -> str:
    """ Example for import CSV files """
    base_qry = _get_base_copy_cmd(table=table, path=path)
    return "{base_query} delimiter '{delimiter}' IGNOREHEADER 1 dateformat as 'auto' timeformat as 'auto'".format(
        base_query=base_qry, delimiter=',')


def build_copy_command(path: str) -> str:
    """ Extracts copy command params based on key """
    qry = _get_csv_copy_cmd(table=tablename, path=path)
    return qry


def lambda_handler(event, context):
    # TODO implement
    for record in event['Records']:
        file_name_with_directory = record['s3']['object']['key']
        file_name = record['s3']['object']['key'].split('/')[0]
        bucketName=record['s3']['bucket']['name']
        print("File Name : ",file_name)
        print("File Name with directory: ",file_name_with_directory)
        print("Bucket Name : ",bucketName)
        local_file_full_name='/tmp/{}'.format(file_name)
        s3 = boto3.client('s3')
        s3.download_file(bucketName, file_name_with_directory, local_file_full_name)
        print("File downloaded successfully")
        
        with ZipFile(local_file_full_name, 'r') as f:
            #extract in current directory
            f.extractall('/tmp/unzip{}'.format(myuuid))
        file_names=''
        for filename in os.listdir('/tmp/unzip{}'.format(myuuid)):
            f = os.path.join('/tmp/unzip{}'.format(myuuid), filename)
            print("File Name : ",f)
            s3.upload_file(f, bucketName, 'raw-data/{}'.format(filename))
            os.remove(f)
            file_names=file_names+','+'s3://{}/raw-data/{}'.format(bucketName,filename)
        
        file_temp = file_names[1:]
        file_names_temp = file_temp.split(',')
        for from_path in file_names_temp:
            print("from path {}".format(from_path))
            connection = psycopg2.connect(dbname = dbname,
                                           host = host,
                                           port = '5439',
                                           user = user,
                                           password = password)
                                           
            print('after connection....')
            curs = connection.cursor()
            print('after cursor....')
            query = build_copy_command(from_path) 
            print("query is {}".format(query))
            print('after query....')
            curs.execute(query)
            connection.commit()
            #print(curs.fetchmany(3))
            print('Query Executed....')
            curs.close()
            print('after curs close....')
            connection.close()
            print('Connection closed....')
            print('Process Completed....')
   
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
