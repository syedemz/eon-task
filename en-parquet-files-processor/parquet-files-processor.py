import json
import boto3
import pandas as pd
from io import BytesIO
import uuid
from utils import handle_aws_exceptions


s3_client = boto3.client('s3')
# S3 bucket and folder names
SOURCE_BUCKET = 'eon-s3silver'

@handle_aws_exceptions
def lambda_handler(event, context):
    """_summary_

    Args:
        event (_type_): _description_
        context (_type_): _description_

    Raises:
        FileNotFoundError: _description_

    Returns:
        _type_: _description_
    """  
    key = event.get('key')
    if not key:
        raise ValueError("Missing 'key' parameter in Step Function input")      
    SOURCE_PREFIX = key
    DEST_PREFIX = key + "combined/"
    # List objects in the source S3 folder
    response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)
    parquet_files = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            parquet_files.append(obj['Key'])
    # Read Parquet files from S3 into DataFrames
    if not parquet_files:
            raise FileNotFoundError("No Parquet files found in the specified S3 folder.")
    dfs = []
    for parquet_file in parquet_files:
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=parquet_file)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        dfs.append(df)
    # Concatenate the DataFrames
    if not dfs:
            raise ValueError("No valid Parquet files could be loaded into DataFrames.")
    
    merged_df = pd.concat(dfs, ignore_index=True)

    # Generate a UUID for the filename
    filename = f"{uuid.uuid4()}.snappy.parquet"

    # Write the merged DataFrame to a Parquet file in memory
    buffer = BytesIO()
    merged_df.to_parquet(buffer, compression='snappy', index=False)
    buffer.seek(0)

    # Upload the merged Parquet file to the destination S3 folder
    dest_key = f"{DEST_PREFIX}{filename}"
    s3_client.put_object(Bucket=SOURCE_BUCKET, Key=dest_key, Body=buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully processed {len(parquet_files)} files and saved to {dest_key}')
    }