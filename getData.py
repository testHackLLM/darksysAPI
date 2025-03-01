import globals
from fastapi import HTTPException
import json
from enum_data import FileNameKey
# from enum import Enum

# class FileNameKey(Enum):
#     OPPORTUNITY_OUTLETS = "harshprincegoogleparser/{}_PJP_opportunitiesOutlets.csv"
#     QUESTIONS = "data/questions.csv"
#     ANSWERS = "data/answers.csv"

def get_file_name(lob: str, fileName: FileNameKey):
    """Returns the file path based on the target file name and lob."""
    
    match fileName:
        case fileName.OPPORTUNITY_OUTLETS:
            file_name = FileNameKey.OPPORTUNITY_OUTLETS.value.format(lob)
        case fileName.QUESTIONS:
            file_name = FileNameKey.QUESTIONS.value.format(lob)
        case fileName.ANSWERS:
            file_name = FileNameKey.ANSWERS.value.format(lob)
        case _:
            file_name = "default_file.csv"  # Fallback case for unknown targetFileName
    
    return file_name


def get_query(login_id: str):
    
    if globals.test:
        query = f'SELECT * FROM s3object s WHERE TRIM(s."LoginId") = \'{login_id}\''
    else:
        query = f'SELECT * FROM s3object s WHERE TRIM(s."loginId") = \'{login_id}\''
    
    return query

def get_query_all():
    if globals.test:
        query = f'SELECT * FROM s3object s'
    else:
        query = f'SELECT * FROM s3object s'
    
    return query

def convert_record_to_json(records_str):
    records_list = []
    for line in records_str.split("\n"):
        if line.strip():  # Avoid empty lines
            json_record = json.loads(line)  
            convert_key_to_json(json_record, "skus")
            records_list.append(json_record)  # Append the modified dictionary
    return records_list

def convert_key_to_json(record: dict, key: str):
    # """Converts a specific key in a dictionary to JSON if it's a string."""
    if key in record and isinstance(record[key], str):
        try:
            record[key] = json.loads(record[key])  # Convert string to JSON
        except json.JSONDecodeError:
            pass  # If it's not a valid JSON string, leave it as is



