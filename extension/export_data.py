import pandas as pd
from sqlalchemy import create_engine
import os

def show_tables(db_url):
    """
    Fetches and displays the names of all tables in the public schema of the database.
    
    Args:
        db_url (str): The database URL for connection (e.g., 'postgresql://user:password@host/dbname').
    
    Returns:
        None
    """
    engine = create_engine(db_url)
    try:
        # Query to fetch all table names in the public schema
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        
        # Read the result into a pandas DataFrame
        tables_df = pd.read_sql(query, engine)
        
        # Display the table names
        print("Tables in the public schema:")
        print(tables_df)
    
    except Exception as e:
        print(f"Error occurred while fetching tables: {e}")

def export_cleaned_data_with_pivot(db_url, table_name, output_file, path="/clean_data", tid_col='_tid_', attr_col='_attribute_', val_col='_value_'):
    """
    Fetches the cleaned data from the specified table, pivots it such that each attribute becomes a column, 
    and each row corresponds to a tid. Exports the pivoted data to a CSV file in the specified path.

    Args:
        db_url (str): The database URL for connection (e.g., 'postgresql://user:password@host/dbname').
        table_name (str): The name of the table containing the cleaned data.
        output_file (str): The file name where the cleaned data will be saved as CSV.
        path (str): The folder path where the cleaned data will be saved (default is '/clean_data').
        tid_col (str): The column representing transaction IDs (default is '_tid_').
        attr_col (str): The column representing attributes (default is '_attribute_').
        val_col (str): The column representing the values of the attributes (default is '_value_').

    Returns:
        None
    """
    # Combine the path and output file name to form the full file path
    full_output_path = os.path.join(path, output_file)

    # Ensure the target directory exists
    os.makedirs(path, exist_ok=True)

    # Create a connection to the database
    engine = create_engine(db_url)

    try:
        # Query to fetch all cleaned data from the specified table
        query = f"SELECT * FROM {table_name}"  # Adjust table name if needed
        
        # Read the data into a pandas DataFrame
        cleaned_df = pd.read_sql(query, engine)

        # Pivot the DataFrame: Each unique attribute becomes a column
        pivoted_df = cleaned_df.pivot(index=tid_col, columns=attr_col, values=val_col)

        # Flatten the columns (optional, if multi-level columns are created)
        pivoted_df.columns = [col for col in pivoted_df.columns]  # Flatten the column names

        # Reset index to avoid the tid_col becoming the index and remove it if present
        pivoted_df.reset_index(drop=True, inplace=True)  # drop=True will not keep the old index as a column

        # Save the pivoted DataFrame to a CSV file without the tid_col
        pivoted_df.to_csv(full_output_path, index=False)  # Don't include the index as a separate column
        print(f"Cleaned and pivoted data has been exported to {full_output_path}")
    
    except Exception as e:
        print(f"Error occurred: {e}")

# Example usage:
db_url = 'postgresql://holocleanuser:abcd1234@localhost/holo'
table_name = 'hospital_clean'
output_file = 'hospital_cleaned_pivoted.csv'
path = "./clean_data"  # Define the directory where you want to export the file

# Show all tables in the 'public' schema first
show_tables(db_url)

# Then export the cleaned and pivoted data
export_cleaned_data_with_pivot(db_url, table_name, output_file, path)
