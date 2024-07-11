import json
from functools import wraps
from botocore.exceptions import ClientError



def handle_aws_exceptions(func):
    """_summary_

    Args:
        func (_type_): _description_

    Returns:
        _type_: _description_
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            print(f"Boto3 client error: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error processing file: {str(e)}")
            }
        except FileNotFoundError as e:
            print(f"FileNotFoundError: {str(e)}")
            return {
                'statusCode': 404,
                'body': json.dumps(f'FileNotFoundError: {str(e)}')
            }
        except ValueError as e:
            print(f"ValueError: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps(f'ValueError: {str(e)}')
            }
        except Exception as e:
            print(f"Unexpected error: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Unexpected error: {str(e)}")
            }
    return wrapper