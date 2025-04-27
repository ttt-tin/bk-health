import pandas as pd
import os
import re
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
from pyathena import connect
import boto3

load_dotenv()
output_bucket = os.getenv('S3_OUTPUT_BUCKET')
database = 'bk_health_lakehouse_db'
region = os.getenv('AWS_REGION')
conn = connect(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
    s3_staging_dir=output_bucket,  # e.g., 's3://my-bucket/athena-output/'
    region_name=region,            # e.g., 'us-east-1'
    schema_name=database           # e.g., 'my_database'
)

def load_mapping_from_athena(db_name, table_name):
    """
    Tải thông tin mapping từ Athena
    Trả về dict: {source_table_name: {standard_column: source_column}}
    """
    try:
        query = f"""
            SELECT db_table, db_column, standard_table, standard_column
            FROM mapping
            WHERE db_name = '{db_name}' AND db_table = '{table_name}'
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        print(f"✅ Executed Athena query:\n{query}")
        print(f"Rows fetched: {rows}")

        mapping_data = {}
        for row in rows:
            db_table, db_column, standard_table, standard_column = row
            db_table = db_table.replace("_repaired", "") if db_table else ""
            if db_table not in mapping_data:
                mapping_data[db_table] = {}
            mapping_data[db_table][standard_column] = db_column

        return mapping_data

    except Exception as e:
        print(f"❌ Error loading mapping from Athena: {e}")
        return {}


def map_all_tables_from_folder(input_folder):
    """
    Chuẩn hóa tất cả các file CSV trong thư mục đầu vào.
    Lưu file vào ./standard nếu mapping thành công, hoặc ./missing_mapping nếu thiếu mapping hoặc key.
    """

    print('map_all_tables_from_folder', input_folder)
    try:
        for root, _, files in os.walk(input_folder):
            print(f"Processing folder: {root}")
            print(f"Files found: {files}")
            for file in files:
                try:
                    if file.endswith('.csv'):
                        input_csv_path = os.path.join(root, file)
                        print(f"Processing file: {input_csv_path}")

                        # Extract source table name
                        source_table_name = os.path.splitext(file)[0]
                        source_table_name = re.sub(r'_data_\d{14}$', '', source_table_name)

                        # Extract db_name and table_name from folder structure
                        relative_path = os.path.relpath(root, input_folder)
                        parts = relative_path.split(os.sep)
                        db_name = parts[0] if len(parts) > 0 and parts[0] != '.' else 'unknown'
                        table_name = parts[1] if len(parts) > 1 else source_table_name

                        # Load mapping
                        table_mapping = load_mapping_from_athena(db_name, table_name).get(table_name, {})
                        print(f"Mapping for {db_name}/{table_name}: {table_mapping}")

                        # Read source data
                        source_data = pd.read_csv(input_csv_path)
                        print(f"CSV columns: {list(source_data.columns)}")

                        # Check if mapping exists
                        if not table_mapping:
                            print(f"⚠️ No mapping found for {db_name}/{table_name}")
                            save_to_missing_mapping(input_folder, root, source_table_name, source_data, reason="No mapping defined")
                            continue

                        # Check for missing source columns, excluding optional ones
                        optional_columns = ['_tid_', 'key_id']
                        missing_columns = [std_col for std_col, src_col in table_mapping.items() if src_col not in source_data.columns and src_col not in optional_columns]
                        if missing_columns:
                            print(f"⚠️ Missing source columns for {db_name}/{table_name}: {missing_columns}")
                            save_to_missing_mapping(input_folder, root, source_table_name, source_data, reason=f"Missing columns: {missing_columns}")
                            continue

                        # Perform mapping with null handling for optional columns
                        try:
                            for standard_column, source_column in table_mapping.items():
                                if source_column in source_data.columns:
                                    # Map existing column, replacing null-like values
                                    source_data[standard_column] = source_data[source_column].replace([np.nan, '', None], pd.NA)
                                else:
                                    # Assign pd.NA for missing optional columns
                                    print(f"⚠️ Source column '{source_column}' not found; assigning pd.NA to '{standard_column}'")
                                    source_data[standard_column] = pd.NA
                        except Exception as e:
                            print(f"❌ Error during mapping: {e}")
                            save_to_missing_mapping(input_folder, root, source_table_name, source_data, reason=f"Mapping error: {e}")
                            continue

                        # Keep only standard columns
                        columns_to_keep = list(table_mapping.keys())
                        standardized_data = source_data[columns_to_keep]

                        # Save standardized data
                        output_folder = os.path.join("./standard", relative_path)
                        os.makedirs(output_folder, exist_ok=True)
                        output_csv_name = f"{source_table_name}_standard_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                        output_csv_path = os.path.join(output_folder, output_csv_name)
                        standardized_data.to_csv(output_csv_path, index=False)

                        print(f"✅ Standardized: {file} -> {output_csv_path}")

                except Exception as e:
                    print(f"❌ Error processing file {file}: {e}")
                    try:
                        source_data = pd.read_csv(input_csv_path)
                        save_to_missing_mapping(input_folder, root, source_table_name, source_data, reason=f"Processing error: {e}")
                    except:
                        print(f"⚠️ Could not save {file} to missing_mapping")

    except Exception as e:
        print(f"❌ General error in map_all_tables_from_folder: {e}")

def save_to_missing_mapping(input_folder, root, source_table_name, source_data, reason="Unknown"):
    """
    Lưu file vào thư mục ./missing_mapping khi thiếu mapping hoặc xảy ra lỗi.
    """
    relative_path = os.path.relpath(root, input_folder)
    output_folder = os.path.join("./missing_mapping", relative_path)
    os.makedirs(output_folder, exist_ok=True)
    output_csv_name = f"{source_table_name}_missing_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    output_csv_path = os.path.join(output_folder, output_csv_name)
    source_data.to_csv(output_csv_path, index=False)
    print(f"⚠️ Saved to missing_mapping: {output_csv_path} (Reason: {reason})")

    upload_missing_mapping_to_s3(input_folder, root, source_table_name, source_data, reason="Unknown")

def upload_missing_mapping_to_s3(input_folder, root, source_table_name, source_data, reason="Unknown"):
    """
    Lưu file lỗi lên S3 bucket 'bk-health-bucket-error' theo folder ngày + giờ: missing_mapping/YYYY/MM/DD/HH/
    """
    try:
        # Generate timestamp
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        timestamp = now.strftime("%Y%m%d%H%M%S")

        # S3 key structure
        relative_path = os.path.relpath(root, input_folder)
        output_csv_name = f"{source_table_name}_missing_{timestamp}.csv"
        s3_folder_path = f"missing_mapping/{year}/{month}/{day}/{hour}/{relative_path}".strip("/")
        s3_key = f"{s3_folder_path}/{output_csv_name}"

        # Save temporary local file
        local_temp_path = f"/tmp/{output_csv_name}"
        source_data.to_csv(local_temp_path, index=False)

        # Upload to S3
        s3 = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        bucket_name = "bk-health-bucket-error"
        s3.upload_file(local_temp_path, bucket_name, s3_key)

        print(f"⚠️ Saved missing mapping to s3://{bucket_name}/{s3_key} (Reason: {reason})")

    except Exception as e:
        print(f"❌ Failed to save missing mapping to S3: {e}")

# Run the script
input_folder_path = "./output"
map_all_tables_from_folder(input_folder_path)