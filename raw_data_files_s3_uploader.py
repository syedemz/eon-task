import json
import os
import time
from datetime import datetime
from stat import S_ISDIR
import paramiko
from utils import handle_sftp_connection_exception, handle_ssh_exceptions, handle_file_exceptions




# SFTP connection details
# change the hostname and username accordingly
HOSTNAME = "s-6fdeab7da1c64c3a9.server.transfer.eu-west-1.amazonaws.com"
USERNAME = "test-user"
PPK_KEY_PATH = "amadeus"

# S3 details
S3_BUCKET = ""

current_date = datetime.now()
yyyy = current_date.strftime("%Y")
mm = current_date.strftime("%m")
dd = current_date.strftime("%d")

@handle_ssh_exceptions
@handle_sftp_connection_exception
def connect_sftp():
    """_summary_

    Returns:
        _type_: _description_
    """
    # Create a new SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Prompt for the private key password
    key_password = "amadeus"
    # Load the private key
    private_key = paramiko.RSAKey.from_private_key_file(PPK_KEY_PATH, password=key_password)
    # Connect to the SFTP server
    ssh.connect(hostname=HOSTNAME, username=USERNAME, pkey=private_key)
    # Open an SFTP session
    sftp = ssh.open_sftp()

    return ssh, sftp

def upload_file(sftp, local_path, remote_path):
    """_summary_

    Args:
        sftp (_type_): _description_
        local_path (_type_): _description_
        remote_path (_type_): _description_
    """
    sftp.put(local_path, remote_path)

def create_remote_dir(sftp, remote_dir):
    """_summary_

    Args:
        sftp (_type_): _description_
        remote_dir (_type_): _description_
    """
    try:
        sftp.stat(remote_dir)
    except IOError:
        sftp.mkdir(remote_dir)

@handle_file_exceptions
def extract_data_asset(filepath):
    """_summary_

    Args:
        filepath (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Open and read the JSON file
    with open(filepath, 'r') as file:
        data = json.load(file)

    # Check if the data is a list and has at least one element
    if isinstance(data, list) and len(data) > 0:
        # Extract the 'dataAsset' field from the first element
        data_asset = data[0].get('dataAsset')
        print(data_asset)
        return data_asset
    else:
        print("Error: The JSON file does not contain a list or the list is empty.")
        return None


def begin_upload(filepath, s3_prefix):
    """_summary_

    Args:
        filepath (_type_): _description_
        s3_prefix (_type_): _description_
    """

    ssh, sftp = connect_sftp()

    if ssh is None or sftp is None:
        print("Failed to establish SFTP connection. Exiting.")
        return

    try:
        # Ensure the remote directory structure exists
        remote_path = os.path.join(S3_BUCKET, s3_prefix)
        dirs = remote_path.split('/')
        for i in range(1, len(dirs)):
            path = '/'.join(dirs[:i+1])
            try:
                sftp.stat(path)
            except IOError:
                create_remote_dir(sftp, path)

        # Upload the file
        act_filename = os.path.basename(filepath)
        local_filename = f"{yyyy}-{mm}-{dd}_{act_filename}"
        remote_file_path = os.path.join(remote_path, local_filename)
        upload_file(sftp, filepath, remote_file_path)
        print(f"File uploaded successfully to {local_filename}")

    finally:
        sftp.close()
        ssh.close()

def generate_formatted_string(data_asset):
    """_summary_

    Args:
        data_asset (_type_): _description_

    Returns:
        _type_: _description_
    """
    return f"/asset-name={data_asset}/yyyy={yyyy}/mm={mm}/dd={dd}"


def get_data_files(directory='data'):
    """_summary_

    Args:
        directory (str, optional): _description_. Defaults to 'data'.

    Returns:
        _type_: _description_
    """
    results = []
    # Check if the directory exists
    if not os.path.isdir(directory):
        print(f"Error: The directory '{directory}' does not exist.")
        return results

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        # Check if it's a file (not a subdirectory)
        if os.path.isfile(filepath):
            print(f"Processing file: {filename}")
            data_asset = extract_data_asset(filepath)
            if data_asset:
                folder_structure = generate_formatted_string(data_asset)
                results.append((filepath, folder_structure))
    return results


def main():
    """_summary_
    """
    while True:
        res = get_data_files()
        for x in res:
            filename, folder = x
            begin_upload(filename, folder)    
        # Sleep for 15 minutes
        time.sleep(900)

if __name__ == "__main__":
    main()