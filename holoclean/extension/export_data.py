import pandas as pd
from sqlalchemy import create_engine
import os

def show_tables(db_url):
    engine = create_engine(db_url)
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        tables_df = pd.read_sql(query, engine)
        
        print("Tables in the public schema:")
        print(tables_df)
    
    except Exception as e:
        print(f"Error occurred while fetching tables: {e}")

def export_cleaned_data_with_pivot(db_url, table_name, output_file, path="/clean_data", tid_col='_tid_', attr_col='_attribute_', val_col='_value_'):
    full_output_path = os.path.join(path, output_file)
    os.makedirs(path, exist_ok=True)
    engine = create_engine(db_url)

    try:
        query = f"SELECT * FROM {table_name}" 
        
        cleaned_df = pd.read_sql(query, engine)

        pivoted_df = cleaned_df.pivot(index=tid_col, columns=attr_col, values=val_col)

        pivoted_df.columns = [col for col in pivoted_df.columns]

        pivoted_df.reset_index(drop=True, inplace=True)

        pivoted_df.to_csv(full_output_path, index=False) 
        print(f"Cleaned and pivoted data has been exported to {full_output_path}")
    
    except Exception as e:
        print(f"Error occurred: {e}")


def export_data(db_url, table_name, output_file, path="./exported_data"):
    full_output_path = os.path.join(path, output_file)
    os.makedirs(path, exist_ok=True)
    engine = create_engine(db_url)

    try:
        query = f"SELECT * FROM {table_name};"
        data_df = pd.read_sql(query, engine)
        if '_tid_' in data_df.columns:
            data_df = data_df.drop(columns=['_tid_'])
        data_df.to_csv(full_output_path, index=False)
        print(f"Dữ liệu từ bảng '{table_name}' đã được xuất ra file: {full_output_path}")
    except Exception as e:
        print(f"Lỗi khi xuất dữ liệu từ bảng '{table_name}': {e}")

db_url = 'postgresql://holocleanuser:abcd1234@localhost/holo'
table_name = 'hospital_repaired'
output_file = 'hospital_repaired.csv'
path = "./clean_data"

show_tables(db_url)

# export_cleaned_data_with_pivot(db_url, table_name, output_file, path)
export_data(db_url, table_name, output_file)
