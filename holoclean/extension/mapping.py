import pandas as pd
import os
import re
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Cấu hình kết nối PostgreSQL
DB_CONFIG = {
    'dbname': os.getenv('HOLO_DB_NAME'),
    'user': os.getenv('HOLO_DB_USER'),
    'password': os.getenv('HOLO_DB_PASSWORD'),
    'host': os.getenv('HOLO_DB_HOST'),
    'port': os.getenv('HOLO_DB_PORT', 5432)
}

def load_mapping_from_postgres(db_name, table_name):
    """
    Tải thông tin mapping từ bảng mapping trong PostgreSQL
    Trả về dict: {source_table_name: {standard_column: source_column}}
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT db_table, db_column, standard_table, standard_column FROM mapping WHERE db_name = %s AND db_table = %s", (db_name, table_name))

    rows = cursor.fetchall()
    conn.close()

    print('rows', rows)

    mapping_data = {}
    for db_table, db_column, _, standard_column in rows:
        db_table = db_table.replace("_repaired", "")
        if db_table not in mapping_data:
            mapping_data[db_table] = {}
        mapping_data[db_table][standard_column] = db_column
    return mapping_data

def map_all_tables_from_folder(input_folder):
    try:
        for root, _, files in os.walk(input_folder):
            for file in files:
                try:
                    if file.endswith('.csv'):
                        input_csv_path = os.path.join(root, file)

                        source_table_name = os.path.splitext(file)[0]
                        source_table_name = re.sub(r'_data_\d{14}$', '', source_table_name)

                        relative_path = os.path.relpath(root, input_folder) 
                        parts = relative_path.split(os.sep)
                        db_name = parts[0] if len(parts) > 0 else 'unknown'
                        table_name = parts[1] if len(parts) > 1 else 'unknown'

                        table_mapping = load_mapping_from_postgres(db_name, 'patient_repaired')
                        print('table_mapping', table_mapping)

                        source_data = pd.read_csv(input_csv_path)

                        if not table_mapping:
                            relative_path = os.path.relpath(root, input_folder)
                            output_folder = os.path.join("./standard", relative_path)
                            os.makedirs(output_folder, exist_ok=True)

                            output_csv_name = f"{source_table_name}_standard_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                            output_csv_path = os.path.join(output_folder, output_csv_name)
                            source_data.to_csv(output_csv_path, index=False)
                            continue

                        # Mapping dữ liệu
                        for standard_column, source_column in table_mapping.items():
                            if source_column in source_data.columns:
                                source_data[standard_column] = source_data[source_column]

                        columns_to_keep = list(table_mapping.keys())
                        standardized_data = source_data[columns_to_keep]

                        # Lưu dữ liệu sau mapping
                        relative_path = os.path.relpath(root, input_folder)
                        output_folder = os.path.join("./standard", relative_path)
                        os.makedirs(output_folder, exist_ok=True)

                        output_csv_name = f"{source_table_name}_standard_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                        output_csv_path = os.path.join(output_folder, output_csv_name)
                        standardized_data.to_csv(output_csv_path, index=False)

                        print(f"✅ Chuẩn hóa: {file} -> {output_csv_path}")

                except Exception as e:
                    print(f"-----")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi chung: {e}")
