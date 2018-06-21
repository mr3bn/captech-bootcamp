import pandas as pd
import boto3
from io import BytesIO


def df_to_bucket(df, bucket_name, obj_name):
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, obj_name)
    response = obj.put(Body=buffer.getvalue(), ACL='public-read')
    return response['ResponseMetadata']['HTTPStatusCode'] == 200


def lambda_handler(event, context):
    # gather bucket data from event object
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    global_path = 'https://s3.amazonaws.com/' + bucket + '/' + key

    # read csv as dataframe
    df = pd.read_csv(global_path)

    # do pandas things
    df['Communication'].fillna('Unknown', inplace=True)
    
    # put to next bucket
    return df_to_bucket(df, '02-cleansed', 'current_campaign.csv')