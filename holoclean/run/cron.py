#!/usr/bin/env python3
import os
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
from pyathena import connect

# Khởi tạo client S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
    region_name=os.getenv('AWS_REGION')
)

def check_exist_id_mapping(database_name, table_name, old_id, new_id, cursor):
    """Kiểm tra xem mapping ID đã tồn tại chưa."""
    try:
        query = f"""
        SELECT COUNT(*) 
        FROM {database_name}.{table_name}_id_mapping 
        WHERE old_id = '{old_id}' AND new_id = '{new_id}'
        """
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        print(f"Error checking ID mapping: {e}")
        return False

def generate_insert_query_for_id_mapping(table_name, mapping_id, database_name, old_id, new_id):
    """Tạo query insert cho bảng ID mapping."""
    return f"""
    INSERT INTO {database_name}.{table_name}_id_mapping (id, database_name, table_name, old_id, new_id)
    VALUES ('{mapping_id}', '{database_name}', '{table_name}', '{old_id}', '{new_id}');
    """

def generate_insert_query_from_db(database, table_name, table_structure, row_data, cursor, database_name):
    """
    Generate a single INSERT INTO query for one row of data with relation checking and ID mapping.
    
    Args:
        database (str): The database name in Athena.
        table_name (str): The target table name.
        table_structure (dict): Dict with column names as keys and data types as values.
        row_data (dict): A single row of data.
        cursor: Athena cursor for executing queries.
        database_name (str): Logical database name for mapping.
    
    Returns:
        tuple: (query, key_id) or (None, None) if failed.
    """
    try:
        KEY_ID = str(uuid.uuid4())
        row_data = dict(row_data)
        if "key_id" not in row_data:
            row_data["key_id"] = KEY_ID

        # 1. Kiểm tra relationships
        select_query = f"""
        SELECT 
            id,
            table_reference AS tableReference,
            table_was_reference AS tableWasReference,
            pri_key AS priKey,
            fo_key AS foKey
        FROM {database}.relationships
        WHERE table_reference = '{table_name}'
        """
        cursor.execute(select_query)
        relation_columns = [desc[0] for desc in cursor.description]
        relations = cursor.fetchall()
        relations_dict = [dict(zip(relation_columns, row)) for row in relations]

        # 2. Cập nhật foreign key dựa trên mapping
        for relation in relations_dict:
            fo_key = relation['foKey']
            if fo_key in row_data and row_data[fo_key]:
                mapping_query = f"""
                SELECT new_id
                FROM {database}.{relation['tableWasReference']}_id_mapping
                WHERE old_id = '{row_data[fo_key]}'
                AND database_name = '{database_name}'
                AND table_name = '{relation['tableWasReference']}'
                """
                cursor.execute(mapping_query)
                mapping_result = cursor.fetchone()
                if mapping_result and mapping_result[0]:
                    row_data[fo_key] = mapping_result[0]  # Cập nhật new_id
                else:
                    print(f"No mapping found for {fo_key} = {row_data[fo_key]}, skipping insert")
                    return None, None  # Nếu không tìm thấy mapping, bỏ qua record này

        # 3. Kiểm tra record đã tồn tại chưa
        get_key_query = f"""
        SELECT *
        FROM {database}.tables
        WHERE table_name = '{table_name}'
        """
        cursor.execute(get_key_query)
        keys_unique = cursor.fetchall()

        if not keys_unique:
            insert_key_query = f"""
            INSERT INTO {database}.tables (id, table_name, column_name)
            VALUES ('{str(uuid.uuid4())}', '{table_name}', 'id');
            """
            cursor.execute(insert_key_query)
            cursor.execute(get_key_query)
            keys_unique = cursor.fetchall()

        old_id = row_data.get('id')
        row_data = {key.lower(): value for key, value in row_data.items()}
        IS_EXIST_RECORD = False

        for key_unique in keys_unique:
            parts = [p.strip().lower() for p in key_unique[2].split(",")]
            if not old_id and parts and len(parts) == 1:
                old_id = row_data.get(parts[0], '')

            where = ''
            for part in parts:
                if part in row_data and row_data[part]:
                    if not where:
                        where = f"{part} = '{row_data[part]}'"
                    else:
                        where += f" AND {part} = '{row_data[part]}'"

            if where:
                exist_record_query = f"""
                SELECT *
                FROM {database}.{table_name}
                WHERE {where}
                """
                cursor.execute(exist_record_query)
                exists_record = cursor.fetchall()
                if exists_record:
                    IS_EXIST_RECORD = True
                    columns = [desc[0] for desc in cursor.description]
                    exists_record = [dict(zip(columns, row)) for row in exists_record][0]
                    check_exist = check_exist_id_mapping(database_name, table_name, old_id, exists_record['key_id'], cursor)
                    if not check_exist:
                        mapping_id_query = generate_insert_query_for_id_mapping(
                            table_name, str(uuid.uuid4()), database_name, old_id, exists_record['key_id']
                        )
                        cursor.execute(mapping_id_query)
                    break

        # 4. Tạo query insert nếu record chưa tồn tại
        if not IS_EXIST_RECORD:
            columns = ", ".join(row_data.keys())
            formatted_values = []
            for value in row_data.values():
                if value is None:
                    formatted_values.append("NULL")
                elif isinstance(value, str):
                    formatted_values.append("'" + value.replace("'", "''") + "'")
                else:
                    formatted_values.append(str(value))

            values = ", ".join(formatted_values)
            query = f"""
            INSERT INTO {database}.{table_name} ({columns})
            VALUES ({values});
            """
            return query.strip(), KEY_ID
        else:
            print(f"Record already exists for {table_name} with old_id: {old_id}")
            return None, None  # Không insert nếu record đã tồn tại

    except Exception as e:
        print(f"Error generating INSERT query for {table_name}: {e}")
        return None, None

def get_all_error_files_from_s3(bucket_name, base_prefix="error_data/"):
    """Lấy tất cả file lỗi từ S3 bắt đầu từ prefix 'error_data/'."""
    try:
        all_files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=base_prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                if obj['Key'].endswith('.json'):
                    all_files.append(obj['Key'])
        if not all_files:
            print(f"No error files found in {base_prefix}")
        return all_files
    except ClientError as e:
        print(f"Error listing S3 objects: {str(e)}")
        return []

def extract_db_table_from_path(file_key):
    """Trích xuất database_name và table_name từ đường dẫn S3."""
    parts = file_key.split('/')
    if len(parts) < 4:
        return None, None
    return parts[1], parts[2]  # hospital1, encounter_repaired

def process_error_data(bucket_name, athena_database, output_bucket, region):
    """Xử lý tất cả dữ liệu lỗi từ S3 và insert vào Athena."""
    error_files = get_all_error_files_from_s3(bucket_name)
    if not error_files:
        return
    
    try:
        conn = connect(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=output_bucket,
            region_name=region,
            schema_name=athena_database
        )
        cursor = conn.cursor()

        for file_key in error_files:
            try:
                database_name, table_name = extract_db_table_from_path(file_key)
                if not database_name or not table_name:
                    print(f"Invalid path format for {file_key}, skipping")
                    continue
                
                obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                error_data = json.loads(obj['Body'].read().decode('utf-8'))
                records = error_data if isinstance(error_data, list) else error_data.get('errors', [])
                if not records:
                    print(f"No records found in {file_key}, skipping")
                    continue
                
                success = True
                table_structure = {"columns": list(records[0].keys())}

                for record in records:
                    print('record', record)
                    insert_query, key_id = generate_insert_query_from_db(
                        athena_database, table_name, table_structure, record, cursor, database_name
                    )
                    if insert_query:
                        try:
                            cursor.execute(insert_query)
                            check_exist = check_exist_id_mapping(database_name, table_name, record.get('id', ''), key_id, cursor)
                            if not check_exist:
                                mapping_id_query = generate_insert_query_for_id_mapping(
                                    table_name, str(uuid.uuid4()), database_name, record.get('id', ''), key_id
                                )
                                cursor.execute(mapping_id_query)
                            print(f"Inserted record into {table_name} with key_id: {key_id}")
                        except Exception as e:
                            print(f"Failed to insert into {table_name}: {e}")
                            success = False
                    else:
                        success = False
                
                if success:
                    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                    print(f"Deleted {file_key} from S3 after successful insert")
                else:
                    print(f"Keeping {file_key} in S3 due to errors")

            except Exception as e:
                print(f"Error processing {file_key}: {str(e)}")
                
    except Exception as e:
        print(f"Error connecting to Athena: {e}")
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    bucket_name = 'bk-health-bucket-landing'
    athena_database = 'athena_db'
    output_bucket = 's3://bk-health-bucket-landing/athena_output/'
    region = os.getenv('AWS_REGION', 'us-east-1')
    process_error_data(bucket_name, athena_database, output_bucket, region)

if __name__ == "__main__":
    required_env_vars = ['AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'AWS_REGION']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Error: Missing environment variables: {', '.join(missing_vars)}")
    else:
        main()