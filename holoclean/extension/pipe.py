import os
import time
import threading
import subprocess
import boto3
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from upload import upload_file_to_nestjs_api, clear_output_folder

load_dotenv()

# AWS S3 Configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
    region_name=os.getenv('AWS_REGION')
)
BUCKET_NAME = 'bk-health-bucket-landing'
POLLING_INTERVAL = 30
BATCH_SIZE = 10
EXTRACTION_SCRIPT = "holoclean/extension/ehr_extract.py"
HOLOCLEAN_SCRIPT = "holoclean/run/script1.sh"

# PostgreSQL Configuration
DB_CONFIG = {
    'dbname': os.getenv('HOLO_DB_NAME'),
    'user': os.getenv('HOLO_DB_USER'),
    'password': os.getenv('HOLO_DB_PASSWORD'),
    'host': os.getenv('HOLO_DB_HOST'),
    'port': 5432
}

# Ensure upload directory exists
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'upload-data')  # Tạo thư mục trong thư mục làm việc hiện tại
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# Database Connection
def init_db():
    print(DB_CONFIG)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            file_key TEXT UNIQUE NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

# List unprocessed files from S3
def list_unprocessed_files():
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        all_files = [obj['Key'] for obj in response.get('Contents', [])]
        unprocessed_files = []
        
        # Check against processed files in DB
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        for file_key in all_files:
            cursor.execute('SELECT file_key FROM processed_files WHERE file_key = %s', (file_key,))
            if cursor.fetchone() is None:
                unprocessed_files.append(file_key)
        cursor.close()
        conn.close()
        return unprocessed_files
    except Exception as e:
        print(f"Error listing files in S3: {e}")
        return []

# Mark file as processed
def mark_file_as_processed(file_key):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO processed_files (file_key) VALUES (%s)', (file_key,))
        conn.commit()
    except Exception as e:
        print(f"Error marking file as processed: {e}")
    finally:
        cursor.close()
        conn.close()

# Download files in batch
def download_batch(files):
    # Thư mục lưu trữ tệp tải xuống
    save_dir = os.path.join(os.getcwd(), 'data', 'structure')
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(save_dir, exist_ok=True)
    
    local_paths = []
    for file_key in files:
        # Lưu tệp vào thư mục 'data/structure'
        local_path = os.path.join(save_dir, os.path.basename(file_key))
        try:
            print(f"Downloading file: {file_key}")
            s3_client.download_file(BUCKET_NAME, file_key, local_path)
            local_paths.append(local_path)
        except Exception as e:
            print(f"Error downloading {file_key}: {e}")
    return local_paths

# Run extract.py
def run_extraction():
    try:
        print("Running extraction script...")
        result = subprocess.run(['python', EXTRACTION_SCRIPT], check=True)
        print("Extraction script completed.")
    except subprocess.CalledProcessError as e:
        print(f"Error running extract.py: {e}")

# Run Holoclean
def run_holoclean():
    try:
        print("Running Holoclean script...")
        # result = subprocess.run(['python', HOLOCLEAN_SCRIPT], check=True)
        process = subprocess.Popen(
            ['bash', HOLOCLEAN_SCRIPT],  # Lệnh bash và các tham số
            stdout=subprocess.PIPE,          # Lấy output
            stderr=subprocess.PIPE,          # Lấy error
            text=True                         # Đảm bảo là string (không phải bytes)
        )

        # Đọc output và error (nếu có)
        stdout, stderr = process.communicate()

        # Kiểm tra exit code để xử lý kết quả
        if process.returncode == 0:
            print("Holoclean script executed successfully.")
            print("Output:", stdout)
        else:
            print(f"Error running script. Exit code: {process.returncode}")
            print("Error Output:", stderr)
        print("Holoclean script completed.")
    except subprocess.CalledProcessError as e:
        print(f"Error running holoclean.py: {e}")



# Upload processed data to S3
def upload_processed_data():
    for root, _, files in os.walk(UPLOAD_FOLDER):
        for file_name in files:
            local_path = os.path.join(root, file_name)
            s3_key = os.path.relpath(local_path, UPLOAD_FOLDER)
            try:
                print(f"Uploading {file_name} to S3...")
                s3_client.upload_file(local_path, BUCKET_NAME, f'processed/{s3_key}')
            except Exception as e:
                print(f"Error uploading {file_name}: {e}")

# Main pipeline
def pipeline():
    while True:
        clear_output_folder('output')
        print("Fetching unprocessed files...")
        unprocessed_files = list_unprocessed_files()
        if not unprocessed_files:
            print("No new files to process. Waiting...")
            time.sleep(POLLING_INTERVAL)
            return
        
        # # Process files in batches
        # batch = unprocessed_files[:BATCH_SIZE]
        # print(f"Processing batch: {batch}")
        # local_files = download_batch(batch)

        # # Mark files as processed (before running extract to avoid duplication)
        # for file_key in batch:
        #     mark_file_as_processed(file_key)

        # # Run extract.py
        # run_extraction()

        # Run Holoclean
        run_holoclean()

        # Upload cleaned data
        upload_processed_data()

        print(f"Batch completed. Waiting {POLLING_INTERVAL} seconds before next batch.")
        time.sleep(POLLING_INTERVAL)

if __name__ == '__main__':
    init_db()
    print("Starting pipeline...")
    pipeline()
