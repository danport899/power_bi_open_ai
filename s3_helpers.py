import boto3
import pandas as pd


def download_csv_from_s3(file_path, bucket):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket= bucket, Key= file_path) 
    df = pd.read_csv(obj['Body'], header=0)
    return df

def save_latest_to_s3(df, source, table, extension='.csv', version='', bucket=''):
    s3_paths = []
    s3_client = boto3.client('s3')

    tmp_csv = f'/tmp/{table}.csv'
    df.to_csv(tmp_csv, index=False)

    file_name = f'{table}{extension}'
    s3_path = f'{source}/table={table}/{file_name}'

    # upload local csv to s3
    s3_client.upload_file(tmp_csv, bucket, s3_path)
    s3_paths.append(s3_path)

    return s3_paths