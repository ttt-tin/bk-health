import pandas as pd

def compare_csv(file1, file2):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    if df1.shape != df2.shape:
        print(df1.shape, df2.shape)
        print("Hai file có kích thước khác nhau. Không thể so sánh toàn diện.")
        return

    differences = []
    for index, (row1, row2) in enumerate(zip(df1.iterrows(), df2.iterrows())):
        if index > 20:
            break;
        diff_columns = {}
        for col in df1.columns:
            val1 = row1[1][col]
            val2 = row2[1][col]

            if str(val1).lower() != str(val2).lower():
                diff_columns[col] = (val1, val2)
        if diff_columns:
            differences.append({"row_index": index, "differences": diff_columns})

    if differences:
        for diff in differences:
            print(f"Row {diff['row_index']} differs:")
            for col, (val1, val2) in diff['differences'].items():
                print(f"  Column '{col}': File1='{val1}', File2='{val2}'")
    else:
        print("Hai file CSV giống nhau hoàn toàn.")

file1_path = "/home/havanchau/Desktop/workspace/STUDY/DA/other/holoclean/exported_data/hospital_repaired.csv"
file2_path = "/home/havanchau/Desktop/workspace/STUDY/DA/other/holoclean/testdata/patient_data_with_roles.csv"

compare_csv(file1_path, file2_path)
