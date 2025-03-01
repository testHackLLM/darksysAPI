import boto3
import pandas as pd
import io
import botocore
from getData import  convert_record_to_json
from enum_data import ReturnDataType
from dotenv import load_dotenv
import os

load_dotenv()

REGION = os.getenv("REGION")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
KEY = os.getenv("KEY")


s3c = boto3.client(
        's3', 
        region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY
    )

def isKeyExist(key):
    try:
        s3c.head_object(Bucket = BUCKET_NAME, Key = key)
        return True
    except botocore.exceptions.ClientError as e:
        return False

def getDfFromS3(key, skip_rows, columns_names, range_value, special_case):
    obj = s3c.get_object(Bucket= BUCKET_NAME , Key = key)
    df = None
    if special_case:
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8',header=None, names=columns_names, usecols=range(range_value), skiprows=skip_rows)
    else:
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return df

def read_file_from_s3(file_key):
    obj = s3c.get_object(Bucket=BUCKET_NAME, Key=file_key)
    
    # Read the file content (assuming it's a text file)
    file_content = obj['Body'].read().decode('utf-8')
    # print(file_content)
    return file_content

def get_data_from_s3(file_name, query, returnType: ReturnDataType):
    # print(returnType)
    response_from_s3 = []

    if not isKeyExist(file_name):
        return []

    resp = s3c.select_object_content(
        Bucket='darksysbucket',
        Key=file_name,
        ExpressionType='SQL',
        Expression=query,
        InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
        OutputSerialization={"JSON": {}},
    )

    for event in resp['Payload']:
        if 'Records' in event:
            raw_data = event["Records"]["Payload"].decode("utf-8")
            # print("Raw Data from S3:\n", raw_data)  
            json_records = convert_record_to_json(raw_data)  
            response_from_s3.extend(json_records)  
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print(f"Stats details bytesScanned: {statsDetails['BytesScanned']}")
            print(f"Stats details bytesProcessed: {statsDetails['BytesProcessed']}")
            print(f"Stats details bytesReturned: {statsDetails['BytesReturned']}")

    # print("Final Parsed Data:\n", response_from_s3)  
    if(returnType == ReturnDataType.ARRAY):
        return response_from_s3
    elif (returnType == ReturnDataType.OBJECT):
        print(response_from_s3)
        return response_from_s3[0]

if __name__ == "__main__":
    lob = "perfettisfai"
    date = "2025-02-13"
    file_name = "dailyTargetReport/dailyUserTarget" + lob + "_" + date + " 18:30:00.csv"
    # query = "SELECT * FROM s3object s WHERE TRIM(s.\"loginid\") = 'E27009S0400'"
    query = "SELECT * FROM s3object s WHERE s.\"LoginId\" = 'C07001S0535'"
    print(file_name, query)
    response = get_data_from_s3(file_name=file_name, query=query)
    print(response)



def save_to_s3(file_name, data, bucket_name=BUCKET_NAME, file_format="csv"):
   
    try:
        if file_format == "csv":
            csv_buffer = io.StringIO()
            data.to_csv(csv_buffer, index=False)
            file_content = csv_buffer.getvalue()
        elif file_format == "json":
            json_buffer = io.StringIO()
            data.to_json(json_buffer, orient="records", lines=True)
            file_content = json_buffer.getvalue()
        elif file_format == "txt":
            file_content = str(data)
        else:
            raise ValueError("Unsupported file format. Use 'csv', 'json', or 'txt'.")

        s3c.put_object(Bucket=bucket_name, Key=file_name, Body=file_content.encode("utf-8"))
        print(f"File '{file_name}' successfully uploaded to S3 bucket '{bucket_name}'.")
    except botocore.exceptions.BotoCoreError as e:
        print(f"Error saving file to S3: {e}")


def isKeyExist(key):
    try:
        s3c.head_object(Bucket = BUCKET_NAME, Key = key)
        return True
    except botocore.exceptions.ClientError as e:
        return False


"""
https://medium.com/@victor.perez.berruezo/download-a-csv-file-from-s3-and-create-a-pandas-dataframe-in-python-ffdb08c2967c

https://towardsthecloud.com/aws-sdk-key-exists-s3-bucket-boto3

https://dev.to/idrisrampurawala/efficiently-streaming-a-large-aws-s3-file-via-s3-select-4on
"""

