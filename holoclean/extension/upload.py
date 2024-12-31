import os
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime
import shutil

def upload_folder_to_s3(folder_path, bucket_name, aws_access_key=None, aws_secret_key=None, region_name='us-east-1'):
    """
    Upload all files in a folder to an S3 bucket with partitioned paths (yyyy/mm/dd/hh).

    :param folder_path: Path to the folder containing files to upload
    :param bucket_name: Bucket name
    :param base_path: Base path in S3 bucket (default: 'uploads')
    :param aws_access_key: AWS access key ID (optional)
    :param aws_secret_key: AWS secret access key (optional)
    :param region_name: AWS region name
    :return: List of uploaded files and their paths in S3
    """
    if aws_access_key and aws_secret_key:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
    else:
        s3_client = boto3.client('s3', region_name=region_name)

    uploaded_files = []

    now = datetime.utcnow()
    partition_path = now.strftime("%Y/%m/%d/%H")

    try:
        for root, _, files in os.walk(folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                object_name = f"{partition_path}/{file_name}"

                s3_client.upload_file(file_path, bucket_name, object_name)
                uploaded_files.append(object_name)
                print(f"Uploaded '{file_name}' to S3 bucket '{bucket_name}' as '{object_name}'.")

        return uploaded_files

    except FileNotFoundError:
        print(f"The folder '{folder_path}' was not found.")
        return []
    except NoCredentialsError:
        print("Credentials not available.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def clear_output_folder(folder_path):
    """
    Deletes all files and folders inside the given folder.

    :param folder_path: Path to the folder to be cleared.
    """
    try:
        if os.path.exists(folder_path):
            # Xóa tất cả file và folder con trong folder chỉ định
            for file_or_folder in os.listdir(folder_path):
                path = os.path.join(folder_path, file_or_folder)
                if os.path.isfile(path) or os.path.islink(path):
                    os.unlink(path)  # Xóa file hoặc symbolic link
                elif os.path.isdir(path):
                    shutil.rmtree(path)  # Xóa folder và nội dung bên trong
            print(f"All contents of '{folder_path}' have been deleted.")
        else:
            print(f"The folder '{folder_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred while clearing the folder: {e}")
