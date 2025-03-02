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
        columns_definitions = []
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

def generate_insert_query_from_db(prefix, table_name, table_structure):
    """
    Tạo câu lệnh INSERT INTO từ dữ liệu bảng.
    """
    try:
        rows = table_structure['data']
        column_names = table_structure['column_names']

        # Chuẩn bị câu lệnh INSERT
        query = f"INSERT INTO \"{prefix}\".\"{table_name}\" ({', '.join(column_names)}) VALUES\n"
        values_list = []

        # Duyệt qua các hàng dữ liệu
        for row in rows:
            values = []
            for value in row:
                if value is None:  # Xử lý NULL
                    values.append("NULL")
                elif isinstance(value, str):  # Xử lý chuỗi
                    # Sử dụng cách thay thế dấu nháy đơn mà không sử dụng f-string
                    values.append("'" + value.replace("'", "''") + "'")
                else:  # Xử lý số, boolean
                    values.append(str(value))


            values_list.append(f"({', '.join(values)})")

        query += ",\n".join(values_list) + ";"
        return query
    except Exception as e:
        print(f"Lỗi khi tạo câu lệnh INSERT INTO: {e}")
        return None

# Ví dụ sử dụng

def execute_athena_query(database, output_bucket, table_name, region):
    # Lấy cấu trúc và dữ liệu của bảng
    table_structure = get_table_structure_and_data(table_name)

    if table_structure:
        # Tạo câu lệnh CREATE TABLE
        create_table_query = generate_create_table_query_from_db(s3_output_database, table_name, table_structure)
        # Tạo câu lệnh INSERT INTO
        insert_query = generate_insert_query_from_db(s3_output_database, table_name, table_structure)

        # Kết nối với Athena
        conn = connect(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=output_bucket,
            region_name=region,
            schema_name=database
        )

        # Thực thi CREATE TABLE và kiểm tra kết quả
        cursor = conn.cursor()
        try:
            cursor.execute(create_table_query)
            print("Table created successfully.")
            
            # Sau khi CREATE TABLE thành công, thực thi INSERT INTO
            cursor.execute(insert_query)
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error occurred: {e}")

# Lặp qua các thư mục trong folder 'standard' (chẳng hạn 'patient', 'condition')
for subfolder in os.listdir(data_folder):
    subfolder_path = os.path.join(data_folder, subfolder)

    # print('')
    
    # Kiểm tra xem đó có phải là thư mục không
    if not os.path.isdir(subfolder_path):
        continue

    print(f"Processing folder: {subfolder}")

    # Merge tất cả các file CSV trong thư mục con
    merged_df = merge_csv_files_in_folder(subfolder_path)

    if merged_df is not None:
        # Lưu dữ liệu đã merge vào một file CSV mới (tuỳ chọn)
        merged_file_path = os.path.join(subfolder_path, f'{subfolder}_merged.csv')
        merged_df.to_csv(merged_file_path, index=False)
        print(f"Đã lưu file đã merge tại: {merged_file_path}")

        # Lấy tên base (patient chẳng hạn)
        base_name = subfolder  # Thường thì tên thư mục là base name, có thể thay đổi theo yêu cầu

        # Lấy file constraint
        constraint_file = os.path.join(constraint_folder, f'{base_name}_constraints.txt')

        # Kiểm tra file constraints tồn tại
        if not os.path.exists(constraint_file):
            print(f"Warning: Constraint file for '{base_name}' not found. Skipping.")
            continue

        print(f"Processing dataset: {base_name}")

        # 2. Load training data và denial constraints
        print(base_name, merged_file_path)
        hc.load_data(base_name, merged_file_path)

        hc.load_dcs(constraint_file)
        hc.ds.set_constraints(hc.get_dcs())

        # 3. Detect erroneous cells using these two detectors
        detectors = [NullDetector(), ViolationDetector()]
        hc.detect_errors(detectors)

        # 4. Repair errors utilizing the defined features
        hc.setup_domain()
        featurizers = [
            InitAttrFeaturizer(),
            OccurAttrFeaturizer(),
            FreqFeaturizer(),
            ConstraintFeaturizer(),
        ]
        hc.repair_errors(featurizers)

        # 5. Evaluate the correctness of the results (optional)
        # hc.evaluate(fpath='../testdata/inf_values_dom.csv',
        #             tid_col='tid',
        #             attr_col='attribute',
        #             val_col='correct_val')

        output_bucket = os.getenv('S3_OUTPUT_BUCKET')
        database = os.getenv('S3_DATABASE')
        region = os.getenv('AWS_REGION')

        execute_athena_query(database, output_bucket, f'{base_name}_repaired', region)

        print(f"Finished processing dataset: {base_name}")

print("All datasets processed.")
