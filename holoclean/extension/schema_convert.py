import boto3
import pandas as pd
import json
import os

# Khởi tạo các client cho Glue và Lake Formation
glue_client = boto3.client(
    'glue',
    region_name='ap-southeast-2',
)
lake_client = boto3.client(
    'lakeformation',
    region_name='ap-southeast-2',
)

# Hàm lấy schema của bảng từ AWS Glue
def get_table_schema(database_name, table_name):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table = response['Table']
        schema = [{"Name": column['Name'], "Type": column['Type']} for column in table['StorageDescriptor']['Columns']]
        return schema
    except Exception as e:
        print(f"Error fetching schema for table {table_name}: {e}")
        return []

# Hàm thực hiện mapping dữ liệu CSV
def map_csv_to_table(input_folder, mapping_json, database_name):
    try:
        # Đọc thông tin mapping từ file JSON
        with open(mapping_json, 'r') as f:
            mapping_data = json.load(f)

        # Lấy danh sách bảng từ Glue
        response = glue_client.get_tables(DatabaseName=database_name)
        table_names = [table['Name'] for table in response['TableList']]
        print(f"Found tables in database '{database_name}': {table_names}")

        # Tạo thư mục đích nếu chưa tồn tại
        output_folder = "./standard"
        os.makedirs(output_folder, exist_ok=True)

        # Duyệt qua từng file CSV trong thư mục
        input_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]
        for input_file in input_files:
            # Tên bảng nguồn lấy từ tên file CSV (bỏ phần mở rộng .csv)
            source_table_name = os.path.splitext(input_file)[0]
            if source_table_name not in table_names:
                print(f"Bảng '{source_table_name}' không tồn tại trong database '{database_name}'. Bỏ qua file '{input_file}'.")
                continue

            # Lấy schema của bảng từ Glue
            schema = get_table_schema(database_name, source_table_name)
            if not schema:
                print(f"Không tìm thấy schema cho bảng '{source_table_name}'. Bỏ qua file '{input_file}'.")
                continue

            # Đọc dữ liệu từ file CSV
            input_csv_path = os.path.join(input_folder, input_file)
            source_data = pd.read_csv(input_csv_path)

            # Lấy thông tin mapping
            table_mapping = next((table for table in mapping_data if table["source_table"] == source_table_name), None)
            if not table_mapping:
                print(f"Không tìm thấy mapping cho bảng '{source_table_name}'. Bỏ qua file '{input_file}'.")
                continue

            mapping_dict = table_mapping["mapping"]

            # Kiểm tra và mapping cột
            columns_to_keep = [col for col in source_data.columns if col in mapping_dict]
            standardized_data = source_data[columns_to_keep].rename(columns=mapping_dict)

            # Kiểm tra kiểu dữ liệu theo schema
            for column in schema:
                col_name = column["Name"]
                col_type = column["Type"]
                if col_name in standardized_data.columns:
                    if col_type == 'string' and not pd.api.types.is_string_dtype(standardized_data[col_name]):
                        print(f"Lỗi: Kiểu dữ liệu cột '{col_name}' không khớp. Dự kiến: string.")
                    elif col_type == 'bigint' and not pd.api.types.is_integer_dtype(standardized_data[col_name]):
                        print(f"Lỗi: Kiểu dữ liệu cột '{col_name}' không khớp. Dự kiến: bigint.")
                    elif col_type == 'double' and not pd.api.types.is_float_dtype(standardized_data[col_name]):
                        print(f"Lỗi: Kiểu dữ liệu cột '{col_name}' không khớp. Dự kiến: double.")

            # Lưu dữ liệu đã chuẩn hóa
            output_csv_name = f"{source_table_name}_standard.csv"
            output_csv_path = os.path.join(output_folder, output_csv_name)
            standardized_data.to_csv(output_csv_path, index=False)

            print(f"Mapping và chuẩn hóa dữ liệu thành công. File kết quả: {output_csv_path}")
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

# Gọi hàm chính
if __name__ == "__main__":
    input_folder_path = "./output"
    mapping_json_path = "mapping.json"
    database_name = "bk_health_lakehouse_db"
    map_csv_to_table(input_folder_path, mapping_json_path, database_name)
