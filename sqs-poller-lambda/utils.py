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
                'body': json.dumps(f"Error processing messages: {str(e)}")
            }
        except Exception as e:
            print(f"Unexpected error: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Unexpected error: {str(e)}")
            }
    return wrapper

def handle_key_json_exceptions(func):
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
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except KeyError as e:
            print(f"Error accessing key in message: {e}")
        except Exception as e:
            print(f"Unexpected error processing message: {e}")
    return wrapper