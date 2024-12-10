import pandas as pd

# Đọc file CSV
file_path = '/home/havanchau/Desktop/workspace/STUDY/DA/other/holoclean/testdata/patient_data.csv'
df = pd.read_csv(file_path)

# Thêm cột 'role' dựa trên giá trị của cột 'gender'
df['role'] = df['gender'].apply(lambda x: 'husband' if x.lower() == 'male' else ('wife' if x.lower() == 'female' else 'unknown'))

# Hiển thị DataFrame sau khi thêm cột
print(df)

# Ghi DataFrame đã chỉnh sửa vào một file CSV mới (nếu cần)
output_file_path = 'patient_data_with_roles.csv'
df.to_csv(output_file_path, index=False)
