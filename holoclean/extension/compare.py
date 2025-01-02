import pandas as pd

def compare_csv(file1, file2, columns_to_compare):
    # Đọc dữ liệu từ hai file CSV
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Kiểm tra sự khác nhau giữa số lượng cột
    if not all(col in df1.columns for col in columns_to_compare) or not all(col in df2.columns for col in columns_to_compare):
        print("Một hoặc cả hai file không chứa tất cả các cột cần so sánh.")
        return

    differences = []
    
    # So sánh từng dòng và các cột được chỉ định
    for index, (row1, row2) in enumerate(zip(df1.iterrows(), df2.iterrows())):
        if index > 20:
            break
        diff_columns = {}
        
        for col in columns_to_compare:
            val1 = row1[1][col]
            val2 = row2[1][col]

            if str(val1).lower() != str(val2).lower() and str(val1).lower() != '_nan_':
                diff_columns[col] = (val1, val2)
        
        if diff_columns:
            differences.append({"row_index": index, "differences": diff_columns})

    if differences:
        for diff in differences:
            print(f"Row {diff['row_index']} differs:")
            for col, (val1, val2) in diff['differences'].items():
                print(f"  Column '{col}': File1='{val1}', File2='{val2}'")
    else:
        print("Các dòng trong các cột so sánh giống nhau hoàn toàn.")

# Đường dẫn đến hai file CSV cần so sánh
file1_path = "exported_data/hospital_repaired.csv"
file2_path = "standard/hospital/hospital_merged.csv"

# Cột cần so sánh
columns_to_compare = ['HospitalName','Address1','City','State','ZipCode','CountyName','PhoneNumber','HospitalType','HospitalOwner','EmergencyService','Condition','MeasureCode','MeasureName','Score','Sample']

compare_csv(file1_path, file2_path, columns_to_compare)
