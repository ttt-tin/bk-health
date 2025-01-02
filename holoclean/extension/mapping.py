import pandas as pd
import json
import os
from datetime import datetime
import re

def map_all_tables_from_folder(input_folder, mapping_json):
    """
    Hàm thực hiện mapping cột giữa các bảng trong các database từ file JSON và giữ nguyên cấu trúc thư mục.

    Args:
        input_folder (str): Đường dẫn đến thư mục chứa các file CSV nguồn.
        mapping_json (str): Đường dẫn đến file JSON chứa thông tin mapping.

    Returns:
        None
    """
    try:
        # Đọc thông tin mapping từ file JSON
        with open(mapping_json, 'r') as f:
            mapping_data = json.load(f)

        print(mapping_data)

        # Lấy thông tin mapping của database duy nhất
        db_mapping = mapping_data  # Giả sử chỉ có một database

        if not db_mapping:
            raise ValueError("Không tìm thấy thông tin mapping cho database.")

        # Duyệt qua tất cả file trong thư mục (bao gồm cả thư mục con)
        for root, _, files in os.walk(input_folder):
            for file in files:
                try: 
                    if file.endswith('.csv'):
                        # Đường dẫn đầy đủ của file CSV
                        input_csv_path = os.path.join(root, file)

                        # Tên bảng nguồn lấy từ tên file CSV (bỏ phần mở rộng .csv)
                        source_table_name = os.path.splitext(file)[0]
                        source_table_name = re.sub(r'_data_\d{14}$', '', source_table_name)

                        # Tìm thông tin mapping cho bảng
                        table_mapping = next((table for table in db_mapping["tables"] if table["source_table"] == source_table_name), None)
                        if not table_mapping:
                            print(f"Không tìm thấy thông tin mapping cho bảng: {source_table_name}. Bỏ qua file {file}.")
                            continue

                        # Lấy từ điển mapping
                        mapping_dict = table_mapping["mapping"]

                        # Đọc dữ liệu từ file CSV
                        source_data = pd.read_csv(input_csv_path)

                        # Kiểm tra và thay đổi các cột theo ánh xạ
                        for standard_column, values in mapping_dict.items():
                            if isinstance(values, list):
                                for check_column in values:
                                    if check_column in source_data.columns:
                                        source_data[standard_column] = source_data[check_column]
                                        break
                            else:
                                if values in source_data.columns:
                                    source_data[standard_column] = source_data[values]

                        # Giữ lại các cột được mapping và áp dụng đổi tên cột
                        columns_to_keep = [col for col in source_data.columns if col in mapping_dict]
                        standardized_data = source_data[columns_to_keep]

                        # Kiểm tra nếu số cột sau khi mapping không đủ như định nghĩa trong JSON thì bỏ qua file
                        expected_columns = len(mapping_dict)
                        actual_columns = standardized_data.columns

                        # Nếu số cột không đủ
                        if len(actual_columns) != expected_columns:
                            # Tìm các cột thiếu
                            missing_columns = [col for col in mapping_dict.keys() if col not in actual_columns]
                            
                            if missing_columns:
                                print(f"File {file} không đủ cột sau khi mapping. Các cột thiếu: {', '.join(missing_columns)}. Bỏ qua file này.")
                            continue

                        # Tạo đường dẫn đích giữ nguyên cấu trúc thư mục con
                        relative_path = os.path.relpath(root, input_folder)
                        output_folder = os.path.join("./standard", relative_path)
                        os.makedirs(output_folder, exist_ok=True)

                        # Tên file đích với thời gian hiện tại để phân biệt
                        output_csv_name = f"{source_table_name}_standard_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                        output_csv_path = os.path.join(output_folder, output_csv_name)

                        # Lưu dữ liệu đã chuẩn hóa vào file CSV đích
                        standardized_data.to_csv(output_csv_path, index=False)

                        print(f"Mapping và chuẩn hóa dữ liệu thành công. File kết quả: {output_csv_path}")
                except Exception as e:
                    print(e)
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")
