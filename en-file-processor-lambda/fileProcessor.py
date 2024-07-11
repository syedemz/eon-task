import json
from datetime import datetime
from io import BytesIO
import boto3
import pandas as pd
from utils import handle_aws_exceptions


s3 = boto3.client('s3')
required_columns = ['timestamp', 'dataAsset', 'iotreadings']
reading_columns = ['timestamp', 'dataAsset']
BUCKET_OUTPUT = 'eon-s3silver'
BUCKET_INPUT = 'eon-s3bronze'

def validate_timestamp(timestamp_str):
    """_summary_

    Args:
        timestamp_str (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        return True
    except ValueError:
        return False
        
def are_all_positive_integers(d):
    """_summary_

    Args:
        d (_type_): _description_

    Returns:
        _type_: _description_
    """
    return all(isinstance(value, int) and value >= 0 for value in d.values())

def validate_iotreadings(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    return df['iotreadings'].apply(are_all_positive_integers).all()



def validate_json_object(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Check for required keys
    columnmatch = set(df.columns) == set(required_columns)
    if columnmatch is False:
        print("not all keys present")
        return False

    # Validate timestamp
    timestamp_valid = df['timestamp'].apply(validate_timestamp)
    if not timestamp_valid.all():
        print("timestamp not correct")
    
    # Validate dataAsset
    dataAsset_valid = df['dataAsset'].apply(lambda x: isinstance(x, str))
    if not dataAsset_valid.all():
        print("something wrong with dataAsset")
    
    # Validate iotreadings
    iotreadings_valid = validate_iotreadings(df.drop(columns=reading_columns).reset_index(drop=True))
    if not iotreadings_valid.all():
        print("something wrong with iot readings")
    return timestamp_valid & dataAsset_valid & iotreadings_valid

@handle_aws_exceptions
def lambda_handler(event, context):
    """_summary_

    Args:
        event (_type_): _description_
        context (_type_): _description_

    Raises:
        ValueError: _description_
        ValueError: _description_

    Returns:
        _type_: _description_
    """

    #key = 'asset-name=jupiter/yyyy=2024/mm=07/dd=10/raw-2.json'
    key = event.get('key')

    if not key:
        raise ValueError("Missing 'key' parameter in Step Function input")
    
    # Read JSON file from S3
    response = s3.get_object(Bucket=BUCKET_INPUT, Key=key)
    json_data = json.loads(response['Body'].read().decode('utf-8'))

    # Validate JSON data
    if not isinstance(json_data, list):
        raise ValueError("JSON data is not a list")
    
    # Convert list of JSON objects to DataFrame
    df = pd.DataFrame(json_data)
    # Perform validation
    valid_mask = validate_json_object(df)
    print(valid_mask)
    valid_data = df[valid_mask]
    if valid_data.empty:
        raise ValueError("No valid data found in the JSON file")
    # Convert to DataFrame
    df = pd.DataFrame(valid_data)
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Write to in-memory buffer as Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    # Write Parquet file back to S3
    output_key = key.rsplit('.', 1)[0] + '.parquet'
    s3.put_object(Bucket=BUCKET_OUTPUT, Key=output_key, Body=buffer.getvalue())
    extract_base_path = lambda key: key.rsplit('/', 1)[0] if len(key.rsplit('/', 1)) > 1 else ""
    base_path = extract_base_path(key)
    base_path = base_path + '/'

    response_body = {
        'statusCode': 200,
        'body': json.dumps(f'Successfully processed {len(valid_data)} records and saved to {output_key}'),
        'key': base_path  # Add the key with base_path value
    }
    return response_body