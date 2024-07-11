import json
from functools import wraps
import paramiko
from botocore.exceptions import ClientError

def handle_ssh_exceptions(func):
    """_summary_: Decorator to handle exceptions during private key loading in connect_sftp.

    Args:
        func (_type_): _description_

    Returns:
        _type_: _description_
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except paramiko.ssh_exception.PasswordRequiredException:
            print("Error: The private key is password-protected, but no password was provided.")
            return None, None
        except paramiko.ssh_exception.SSHException as e:
            print(f"Error loading private key: {str(e)}")
            return None, None
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            return None, None
    return wrapper


def handle_sftp_connection_exception(func):
    """_summary_: Decorator to handle connection issues to the sftp host.

    Args:
        func (_type_): _description_

    Returns:
        _type_: _description_
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except paramiko.ssh_exception.AuthenticationException:
            print("Authentication failed. Please check your username and private key.")
            return None, None
    return wrapper


def handle_file_exceptions(func):
    """_summary_: Decorator to handle issues with file reads.

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
            print(f"Error: Invalid JSON format in the file: {str(e)}")
        except FileNotFoundError:
            print(f"Error: The file was not found: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
        return None
    return wrapper


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
        except ValueError as e:
            print(f"Boto3 client error: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error: {str(e)}")
            }
        except Exception as e:
            print(f"Unexpected error: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Unexpected error: {str(e)}")
            }
    return wrapper
