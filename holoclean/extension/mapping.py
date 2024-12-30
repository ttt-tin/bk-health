import pandas as pd
import json
import os

def map_all_tables_from_folder(input_folder, mapping_json, database_name):
    """
    Hàm thực hiện mapping cột giữa các bảng trong các database từ file JSON.

    Args:
        input_folder (str): Đường dẫn đến thư mục chứa các file CSV nguồn.
        mapping_json (str): Đường dẫn đến file JSON chứa thông tin mapping.
        database_name (str): Tên database để lấy thông tin mapping.

    Returns:
        None
    """
    try:
        # Đọc thông tin mapping từ file JSON
        with open(mapping_json, 'r') as f:
            mapping_data = json.load(f)

        # Tìm thông tin mapping tương ứng với database_name
        db_mapping = next((db for db in mapping_data if db["database"] == database_name), None)
        if not db_mapping:
            raise ValueError(f"Không tìm thấy thông tin mapping cho database: {database_name}")

        # Lấy danh sách các file CSV trong thư mục nguồn
        input_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]

        # Tạo thư mục đích nếu chưa tồn tại
        output_folder = "./standard"
        os.makedirs(output_folder, exist_ok=True)

        # Duyệt qua từng file CSV trong thư mục
        for input_file in input_files:
            # Đường dẫn đầy đủ của file CSV
            input_csv_path = os.path.join(input_folder, input_file)
            # Tên bảng nguồn lấy từ tên file CSV (bỏ phần mở rộng .csv)
            source_table_name = os.path.splitext(input_file)[0]

            # Tìm thông tin mapping cho bảng
            table_mapping = next((table for table in db_mapping["tables"] if table["source_table"] == source_table_name), None)
            if not table_mapping:
                print(f"Không tìm thấy thông tin mapping cho bảng: {source_table_name}. Bỏ qua file {input_file}.")
                continue

            # Lấy từ điển mapping
            mapping_dict = table_mapping["mapping"]

            # Đọc dữ liệu từ file CSV
            source_data = pd.read_csv(input_csv_path)

            # Giữ lại các cột được mapping và áp dụng đổi tên cột
            columns_to_keep = [col for col in source_data.columns if col in mapping_dict]
            standardized_data = source_data[columns_to_keep].rename(columns=mapping_dict)

            # Tên file đích
            output_csv_name = f"{source_table_name}_standard.csv"
            output_csv_path = os.path.join(output_folder, output_csv_name)

            # Lưu dữ liệu đã chuẩn hóa vào file CSV đích
            standardized_data.to_csv(output_csv_path, index=False)

            print(f"Mapping và chuẩn hóa dữ liệu thành công. File kết quả: {output_csv_path}")
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

# Ví dụ sử dụng hàm
input_folder_path = "./output"
mapping_json_path = "mapping.json"
database_name = "db1"

map_all_tables_from_folder(input_folder_path, mapping_json_path, database_name)
