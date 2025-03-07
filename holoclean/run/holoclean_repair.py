import logging
import os
import re
import sys
import holoclean
import pandas as pd
from detect import NullDetector, ViolationDetector
from repair.featurize import *
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from pyathena import connect
import uuid

load_dotenv()

# Thêm đường dẫn cho HoloClean
data_folder = 'standard'
constraint_folder = 'constraint'
s3_output_database = os.getenv('S3_DATABASE')

# 1. Setup a HoloClean session
hc = holoclean.HoloClean(
    db_name='holo',
    domain_thresh_1=0,
    domain_thresh_2=0,
    weak_label_thresh=0.99,
    max_domain=10000,
    cor_strength=0.6,
    nb_cor_strength=0.8,
    epochs=10,
    weight_decay=0.01,
    learning_rate=0.001,
    threads=1,
    batch_size=1,
    verbose=True,
    timeout=3*60000,
    feature_norm=False,
    weight_norm=False,
    print_fw=True
).session

if not os.path.exists(data_folder):
    print(f"Error: Data folder '{data_folder}' does not exist.")
    sys.exit(1)

# Merge tất cả các file CSV trong thư mục con
def merge_csv_files_in_folder(folder_path):
    # Tạo danh sách các file CSV trong thư mục con
    csv_files = [
        f for f in os.listdir(folder_path) 
        if re.match(r'.*_standard_\d{14}\.csv$', f)
    ]

    if not csv_files:
        print(f"Không có file CSV nào trong thư mục: {folder_path}.")
        return None

    # Đọc và merge tất cả các file CSV
    merged_df = pd.DataFrame()

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        try:
            # Đọc file CSV vào DataFrame
            df = pd.read_csv(file_path)
            # Merge vào DataFrame tổng hợp
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            print(f"Đã merge file: {file}")
        except Exception as e:
            print(f"Error khi đọc file {file}: {e}")
    
    return merged_df

def get_table_structure_and_data(table_name):
    """
    Lấy cấu trúc bảng (cột và kiểu dữ liệu) và dữ liệu từ bảng PostgreSQL.
    """
    connection = None
    try:
        # Kết nối với cơ sở dữ liệu
        pwd = os.getenv('HOLO_DB_PASSWORD')
        host = os.getenv('HOLO_DB_HOST')
        user = os.getenv('HOLO_DB_USER')
        port = os.getenv('HOLO_DB_PORT')
        database = os.getenv('HOLO_DB_NAME')

        if not all([pwd, host, user, port, database]):
            raise ValueError("Thiếu thông tin kết nối cơ sở dữ liệu trong biến môi trường.")

        connection = psycopg2.connect(
            database=database,
            user=user,
            password=pwd,
            host=host,
            port=port
        )
        cursor = connection.cursor()

        # Truy vấn cấu trúc bảng
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        columns = cursor.fetchall()

        if not columns:
            raise ValueError(f"Không tìm thấy bảng '{table_name}'.")

        # Lấy dữ liệu bảng
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        col_names = [col[0] for col in columns]

        return {
            "columns": columns,
            "data": rows,
            "column_names": col_names
        }
    except Exception as e:
        print(f"Lỗi: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def generate_create_table_query_from_db(prefix, table_name, table_structure):
    """
    Tạo câu lệnh CREATE TABLE từ cấu trúc bảng.
    """
    try:
        columns_definitions = ["key_id STRING"]
        for column_name, data_type in table_structure['columns']:
            if data_type in ["character varying", "text"]:
                sql_type = "STRING"
            elif data_type in ["integer", "bigint", "smallint"]:
                sql_type = "INT"
            elif data_type in ["double precision", "real", "numeric"]:
                sql_type = "FLOAT"
            elif data_type == "boolean":
                sql_type = "BOOLEAN"
            elif data_type.startswith("timestamp"):
                sql_type = "TIMESTAMP"
            else:
                sql_type = data_type.upper()

            columns_definitions.append(f"`{column_name}` {sql_type}")

        create_table_query = f"CREATE TABLE IF NOT EXISTS `{prefix}`.`{table_name}` (\n  " + ",\n  ".join(columns_definitions) + "\n) LOCATION 's3://bk-health-bucket-trusted/' TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');"
        return create_table_query
    except Exception as e:
        print(f"Lỗi khi tạo câu lệnh CREATE TABLE: {e}")
        return None

# def generate_insert_query_from_db(prefix, table_name, table_structure):
#     """
#     Tạo câu lệnh INSERT INTO từ dữ liệu bảng.
#     """
#     try:
#         rows = table_structure['data']
#         column_names = table_structure['column_names']

#         # Chuẩn bị câu lệnh INSERT
#         query = f"INSERT INTO \"{prefix}\".\"{table_name}\" ({', '.join(column_names)}) VALUES\n"
#         values_list = []

#         # Duyệt qua các hàng dữ liệu
#         for row in rows:
#             values = []
#             for value in row:
#                 if value is None:  # Xử lý NULL
#                     values.append("NULL")
#                 elif isinstance(value, str):  # Xử lý chuỗi
#                     # Sử dụng cách thay thế dấu nháy đơn mà không sử dụng f-string
#                     values.append("'" + value.replace("'", "''") + "'")
#                 else:  # Xử lý số, boolean
#                     values.append(str(value))


#             values_list.append(f"({', '.join(values)})")

#         query += ",\n".join(values_list) + ";"
#         return query
#     except Exception as e:
#         print(f"Lỗi khi tạo câu lệnh INSERT INTO: {e}")
#         return None

def generate_insert_query_from_db(database, table_name, table_structure, row_data):
    """
    Generate a single INSERT INTO query for one row of data.
    
    Args:
        database (str): The database name.
        table_name (str): The target table name.
        table_structure (dict): Dict with column names as keys and data types as values.
        row_data (tuple/list): A single row of data matching the table structure.
    
    Returns:
        str: A single INSERT INTO query.
    """
    try:
        KEY_ID = str(uuid.uuid4())
        row_data = dict(row_data)
        if "key_id" not in row_data:
            row_data["key_id"] = KEY_ID
        
        columns = ", ".join(row_data.keys())
        
        formatted_values = []
        for value in row_data.values():
            if value is None:
                formatted_values.append("NULL")  # Giá trị NULL
            elif isinstance(value, str):
                formatted_values.append("'" + value.replace("'", "''") + "'")

            else:
                formatted_values.append(str(value))  # Giá trị số giữ nguyên

        values = ", ".join(formatted_values)

        query = f"""
        INSERT INTO {database}.{table_name} ({columns})
        VALUES ({values});
        """
        return query.strip(), KEY_ID
    
    except Exception as e:
        print(f"Lỗi khi tạo câu lệnh INSERT INTO: {e}")
        return None, None

def execute_athena_query(database, output_bucket, table_name, region, database_name):
    table_structure = get_table_structure_and_data(table_name)

    if table_structure:
        # Connect to Athena
        try:
            conn = connect(
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
                s3_staging_dir=output_bucket,  # e.g., 's3://my-bucket/athena-output/'
                region_name=region,            # e.g., 'us-east-1'
                schema_name=database           # e.g., 'my_database'
            )

            # Execute CREATE TABLE and check result
            create_table_query = generate_create_table_query_from_db(database, table_name, table_structure)
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            print("Table created successfully.")

            
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
            relations = cursor.fetchall()
            columns = table_structure["columns"]
            records = [dict(zip(columns, record)) for record in table_structure["data"]]
            records_dict = [{key[0]: value for key, value in record.items()} for record in records]

            # records = table_structure['data']
            missing_ref_data = []
            for record in records_dict:
                print('record', record)
                for relation in relations:
                    #####################################
                    # Implement logic update related id #
                    #####################################

                    mapping_data_query = f"""
                        SELECT *
                        FROM {database}.{relation['tableWasReference']}_id_mapping
                        WHERE old_id = '{record[relation['foKey']]}'
                        AND database_name = '{database_name}'
                        AND table_name = '{table_name}'
                    """

                    cursor.execute(mapping_data_query)
                    mapping_result = cursor.fetchone()

                    if mapping_result:
                        exists_record = [dict(zip(columns, row)) for row in mapping_result]
                        print(f"Found mapping for record: {record}")
                        record[relation['foKey']] = exists_record['new_id']
                    else:
                        # If no mapping data found, add the current record to error_records
                        missing_ref_data.append(record)

                #########################################
                # Implement logic to check exist record #
                #########################################

                
                get_key_query = f"""
                SELECT 
                    *
                FROM {database}.tables
                WHERE table_name = '{table_name}'
                """

                cursor.execute(get_key_query)
                keys_unique = cursor.fetchall()

                IS_EXIST_RECORD = False

                print('keys_unique', keys_unique[0][2])
                old_id = None

                for key_unique in keys_unique:
                    parts = [p.strip() for p in key_unique[2].split(",")]
                    if len(parts) == 1:
                        old_id = record[parts[0]]
                    where = ''
                    for part in parts:
                        try:
                            if (record[part]):
                                print('record', record)
                                if len(where) == 0:
                                    where = f"{part} = '{record[part]}'"
                                else:
                                    where = where + f"AND {part} = '{record[part]}'"
                        except KeyError:
                            print(f"⚠️ Key '{part}' không tồn tại trong record: {record}")
                        except Exception as e:
                            print(f"⚠️ Lỗi khác xảy ra: {e}")
                    exist_record_query = f"""
                    SELECT 
                        *
                    FROM {database}.{table_name}
                    WHERE {where}
                    """

                    if (len(where)):
                        print('exist_record_query', exist_record_query)
                        cursor.execute(exist_record_query)
                        exists_record = cursor.fetchall()  # Chỉ gọi fetchall() 1 lần
                        if exists_record:
                            columns = [desc[0] for desc in cursor.description]
                            exists_record = [dict(zip(columns, row)) for row in exists_record]  # Dùng dữ liệu đã lấy

                            IS_EXIST_RECORD = len(exists_record) > 0
                            print('test 2', table_name, str(uuid.uuid4()), database_name, old_id, exists_record[0])
                            check_exist = check_exist_id_mapping(table_name, old_id, exists_record[0]['key_id'])
                            if not check_exist:
                                
                                mapping_id_query = generate_insert_query_for_id_mapping(table_name, str(uuid.uuid4()), database_name, old_id, exists_record[0]['key_id'])
                                print('mapping_id_query', mapping_id_query)
                                cursor.execute(mapping_id_query)
                if not IS_EXIST_RECORD:
                    # After CREATE TABLE succeeds, execute INSERT INTO
                    print('test 1')
                    insert_query, key_id = generate_insert_query_from_db(database, table_name, table_structure, record)
                    check_exist = check_exist_id_mapping(table_name, old_id, key_id)
                    if not check_exist:
                        mapping_id_query = generate_insert_query_for_id_mapping(table_name, str(uuid.uuid4()), database_name, old_id, key_id)
                        cursor.execute(mapping_id_query)
                    cursor.execute(insert_query)

            print("Data inserted successfully.")
        
        except Exception as e:
            print(f"Error occurred: {e}")
        
        finally:
            # Clean up connection
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()


def setup_mapping_table(database_name, table_name):
    """Create mapping table if not exists"""
    connection = None
    try:
        output_bucket = os.getenv('S3_OUTPUT_BUCKET')
        database = os.getenv('S3_DATABASE')
        region = os.getenv('AWS_REGION')
            
        conn = connect(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=output_bucket,  # e.g., 's3://my-bucket/athena-output/'
            region_name=region,            # e.g., 'us-east-1'
            schema_name=database           # e.g., 'my_database'
        )

        cursor = conn.cursor()
        
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS hospital_data.{table_name}_repaired_id_mapping (
                id STRING,
                database_name STRING,
                old_id STRING,
                new_id STRING,
                table_name STRING,
                created_at STRING
            ) LOCATION 's3://bk-health-bucket-trusted/' TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');
        """
        print("create_table_query", create_table_query)
        cursor.execute(create_table_query)

    except Exception as e:
        logging.error(f"Error creating mapping table: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def check_exist_id_mapping(table_name, old_id, new_id):
    """
    Kiểm tra xem bản ghi với old_id và new_id có tồn tại trong bảng {table_name}_id_mapping không.
    
    Args:
        table_name (str): Tên bảng (tiền tố trước "_id_mapping").
        old_id (str): ID cũ cần kiểm tra.
        new_id (str): ID mới cần kiểm tra.
    
    Returns:
        bool: True nếu bản ghi tồn tại, False nếu không tồn tại.
    """
    try:
        database = os.getenv('S3_DATABASE')
        region = os.getenv('AWS_REGION')
        output_bucket = os.getenv('S3_OUTPUT_BUCKET')

        conn = connect(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=output_bucket,
            region_name=region,
            schema_name=database
        )
        cursor = conn.cursor()

        query = f"""
        SELECT COUNT(*) FROM hospital_data.{table_name}_id_mapping
        WHERE old_id = '{old_id}' AND new_id = '{new_id}' AND table_name = '{table_name}';
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        return result[0] > 0 if result else False
    
    except Exception as e:
        logging.error(f"Lỗi khi kiểm tra tồn tại ID mapping: {e}")
        return False
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def generate_insert_query_for_id_mapping(table_name, id_value, database_name, old_id, new_id):
    """
    Generate an INSERT INTO query for the {table_name}_id_mapping table.

    Args:
        table_name (str): The target table name (prefix before "_id_mapping").
        id_value (int): The ID value.
        database_name (str): The name of the database.
        old_id (str): The old identifier.
        new_id (str): The new identifier.

    Returns:
        tuple: (INSERT INTO query string, created_at timestamp)
    """
    try:
        created_at = datetime.now().strftime('%Y%m%d%H%M%S')

        # Tên bảng đầy đủ
        full_table_name = f"hospital_data.{table_name}_id_mapping"
        
        # Format giá trị để tránh lỗi SQL
        database_name_safe = database_name.replace("'", "''") if database_name else None
        old_id_safe = old_id.replace("'", "''") if old_id else None
        new_id_safe = new_id.replace("'", "''") if new_id else None
        table_name_safe = table_name.replace("'", "''")
        created_at_safe = created_at  # Giữ nguyên nếu không cần xử lý

        formatted_values = [
            f"'{id_value}'",
            f"'{database_name_safe}'" if database_name_safe else "NULL",
            f"'{old_id_safe}'" if old_id_safe else "NULL",
            f"'{new_id_safe}'" if new_id_safe else "NULL",
            f"'{table_name_safe}'",
            f"'{created_at_safe}'"
        ]


        
        values = ", ".join(formatted_values)

        query = f"""
        INSERT INTO {full_table_name} (id, database_name, old_id, new_id, table_name, created_at)
        VALUES ({values});
        """

        print('query', query)
        return query.strip()

    except Exception as e:
        print(f"Eror shrvbskvsivbs INSERT INTO: {e}")
        return None

def save_error_data(df, database_name, table_name, error_message):
    """Save errored data to error folder"""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    error_path = os.path.join(error_folder, database_name, table_name)
    os.makedirs(error_path, exist_ok=True)
    
    error_file = f"{error_path}/error_{timestamp}.csv"
    df.to_csv(error_file, index=False)
    logging.error(f"Data saved to {error_file}: {error_message}")
    return error_file

def get_relation_id(df, table_name, database_name, old_id):
    """Update relation IDs based on mapping table"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Get mapping for this table
        cursor.execute("""
            SELECT old_id, new_id 
            FROM id_mapping 
            WHERE table_name = %s
            AND database_name = %s
            AND old_id = %s
        """, (table_name, database_name, old_id))
        
        mapping = {row[0]: row[1] for row in cursor.fetchall()}
        
        return mapping[1]
    except Exception as e:
        logging.error(f"Error updating relation IDs: {e}")
        return False
    finally:
        if connection:
            connection.close()

def get_db_connection():
    """Get database connection"""
    pwd = os.getenv('HOLO_DB_PASSWORD')
    host = os.getenv('HOLO_DB_HOST')
    user = os.getenv('HOLO_DB_USER')
    port = os.getenv('HOLO_DB_PORT')
    database = os.getenv('HOLO_DB_NAME')
    
    return psycopg2.connect(
        database=database,
        user=user,
        password=pwd,
        host=host,
        port=port
    )

def process_and_insert_data(merged_df, base_name, output_bucket, database, region):
    """Process data and insert with mapping"""
    try:
        # Process with HoloClean
        hc.load_data(base_name, merged_df)
        hc.load_dcs(os.path.join(constraint_folder, f'{base_name}_constraints.txt'))
        hc.ds.set_constraints(hc.get_dcs())
        
        detectors = [NullDetector(), ViolationDetector()]
        hc.detect_errors(detectors)
        
        featurizers = [
            InitAttrFeaturizer(),
            OccurAttrFeaturizer(),
            FreqFeaturizer(),
            ConstraintFeaturizer(),
        ]
        hc.repair_errors(featurizers)
        
        # Get processed data
        repaired_df = hc.get_repaired_dataframe()
        
        # Update relation IDs
        repaired_df = update_relation_ids(repaired_df, base_name)
        
        # Save mapping
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Assuming there's an 'id' column in the original data
        if 'id' in repaired_df.columns:
            for _, row in repaired_df.iterrows():
                old_id = row['id']
                new_id = str(uuid.uuid4())  # Generate new UUID
                cursor.execute("""
                    INSERT INTO id_mapping (database_name, old_id, new_id, table_name)
                    VALUES (%s, %s, %s, %s)
                """, (database, old_id, new_id, base_name))
                repaired_df.loc[repaired_df['id'] == old_id, 'id'] = new_id
        
        # Insert to Athena
        conn = connect(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=output_bucket,
            region_name=region,
            schema_name=database
        )
        
        cursor = conn.cursor()
        table_name = f'{base_name}_repaired'

        create_query = generate_create_table_query_from_db(database, table_name, repaired_df)
        cursor.execute(create_query)

        select_query = f"""
            SELECT 
                id,
                table_reference AS tableReference,
                table_was_reference AS tableWasReference,
                pri_key AS priKey,
                fo_key AS foKey
            FROM {database}.relationships
            WHERE table_reference = '{base_name}'
        """

        cursor.execute(select_query)
        rows = cursor.fetchall()
        for row in rows:
            print(f"ID: {row[0]}, TableReference: {row[1]}, TableWasReference: {row[2]}, PriKey: {row[3]}, FoKey: {row[4]}")
        
        # Create table and insert data
        insert_query, key_id = generate_insert_query_from_db(database, table_name, repaired_df)
        
        cursor.execute(insert_query)
        
        connection.commit()
        logging.info(f"Successfully processed and inserted data for {base_name}")
        print(f"Processed database: {database}, table: {table_name}")
        
    except Exception as e:
        error_file = save_error_data(repaired_df, database, base_name, str(e))
        logging.error(f"Error processing {base_name}: {e}")
    finally:
        if connection:
            connection.close()

def cron_job_reprocess_errors():
    """Cron job to reprocess error data"""
    for database_name in os.listdir(error_folder):
        db_path = os.path.join(error_folder, database_name)
        if not os.path.isdir(db_path):
            continue
            
        for table_name in os.listdir(db_path):
            table_path = os.path.join(db_path, table_name)
            for error_file in os.listdir(table_path):
                file_path = os.path.join(table_path, error_file)
                
                try:
                    df = pd.read_csv(file_path)
                    output_bucket = os.getenv('S3_OUTPUT_BUCKET')
                    region = os.getenv('AWS_REGION')
                    
                    process_and_insert_data(df, table_name, output_bucket, database_name, region)
                    
                    # If successful, remove the error file
                    os.remove(file_path)
                    logging.info(f"Successfully reprocessed and removed {file_path}")
                except Exception as e:
                    logging.error(f"Failed to reprocess {file_path}: {e}")
# Main execution

def merge_csv_files_in_folder(folder_path):
    """
    Merge tất cả các file CSV trong thư mục con.
    """
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"Không có file CSV nào trong thư mục: {folder_path}.")
        return None

    # Đọc từng file CSV vào danh sách các DataFrame
    dfs = [pd.read_csv(os.path.join(folder_path, f)) for f in csv_files]

    # Merge tất cả các DataFrame
    merged_df = pd.concat(dfs, ignore_index=True)
    
    print(f"Đã merge {len(csv_files)} file trong thư mục: {folder_path}")
    return merged_df


for database_name in os.listdir(data_folder):
    database_path = os.path.join(data_folder, database_name)
    
    if not os.path.isdir(database_path):
        continue
    
    print(f"Processing database: {database_name}")
    
    for subfolder in os.listdir(database_path):
        subfolder_path = os.path.join(database_path, subfolder)
        
        if not os.path.isdir(subfolder_path):
            continue
        
        print(f"Processing table: {subfolder} in database {database_name}")
        merged_df = merge_csv_files_in_folder(subfolder_path)
        
        if merged_df is not None:

            merged_file_path = os.path.join(subfolder_path, f'{subfolder}_merged.csv')
            merged_df.to_csv(merged_file_path, index=False)
            print(f"Đã lưu file đã merge tại: {merged_file_path}")

            base_name = subfolder

            # constraint_file = os.path.join(constraint_folder, f'{base_name}_constraints.txt')

            # if not os.path.exists(constraint_file):
            #     print(f"Warning: Constraint file for '{base_name}' not found. Skipping.")
            #     continue

            # print(f"Processing dataset: {base_name}")

            # # 2. Load training data và denial constraints
            # print(base_name, merged_file_path)
            # hc.load_data(base_name, merged_file_path)

            # hc.load_dcs(constraint_file)
            # hc.ds.set_constraints(hc.get_dcs())

            # # 3. Detect erroneous cells using these two detectors
            # detectors = [NullDetector(), ViolationDetector()]
            # hc.detect_errors(detectors)

            # # 4. Repair errors utilizing the defined features
            # hc.setup_domain()
            # featurizers = [
            #     InitAttrFeaturizer(),
            #     OccurAttrFeaturizer(),
            #     FreqFeaturizer(),
            #     ConstraintFeaturizer(),
            # ]
            # hc.repair_errors(featurizers)

            # # 5. Evaluate the correctness of the results (optional)
            # # hc.evaluate(fpath='../testdata/inf_values_dom.csv',
            # #             tid_col='tid',
            # #             attr_col='attribute',
            # #             val_col='correct_val')

            setup_mapping_table(database_name, base_name)


            output_bucket = os.getenv('S3_OUTPUT_BUCKET')
            database = os.getenv('S3_DATABASE')
            region = os.getenv('AWS_REGION')

            execute_athena_query(database, output_bucket, f'{base_name}_repaired', region, database_name)

            print(f"Finished processing dataset: {base_name}")

print("All datasets processed.")
