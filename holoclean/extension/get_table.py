import boto3
from dotenv import load_dotenv
import os

load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
glue_region = os.getenv('AWS_REGION')
lake_region = os.getenv('AWS_REGION')

# Khởi tạo các client cho Glue và Lake Formation
glue_client = boto3.client('glue', region_name='ap-southeast-2')
lake_client = boto3.client('lakeformation', region_name='ap-southeast-2')

# Tên database cần truy cập
database_name = "bk_health_lakehouse_db"

# Lấy danh sách bảng trong database
def list_tables(database_name):
    try:
        response = glue_client.get_tables(DatabaseName=database_name)
        tables = response['TableList']
        return [table['Name'] for table in tables]
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return []

# Lấy schema của bảng
def get_table_schema(database_name, table_name):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table = response['Table']
        
        schema = []
        for column in table['StorageDescriptor']['Columns']:
            schema.append({"Name": column['Name'], "Type": column['Type']})
        return schema
    except Exception as e:
        print(f"Error fetching schema for table {table_name}: {e}")
        return []

# Lấy quyền truy cập bảng từ Lake Formation
def list_table_permissions(database_name, table_name):
    try:
        response = lake_client.list_permissions(
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            }
        )
        
        permissions = []
        for permission in response['PrincipalResourcePermissions']:
            permissions.append({
                "Principal": permission['Principal'],
                "Permissions": permission['Permissions']
            })
        return permissions
    except Exception as e:
        print(f"Error fetching permissions for table {table_name}: {e}")
        return []

# Hiển thị chi tiết từng bảng
def list_tables_with_details(database_name):
    tables = list_tables(database_name)
    if not tables:
        print(f"No tables found in database '{database_name}'.")
        return

    print(f"Tables in database '{database_name}':")
    for table_name in tables:
        print(f"\nTable: {table_name}")
        
        # Lấy schema
        schema = get_table_schema(database_name, table_name)
        if schema:
            print("Schema:")
            for column in schema:
                print(f"- Column: {column['Name']}, Type: {column['Type']}")
        else:
            print("No schema found.")

        # Lấy quyền
        permissions = list_table_permissions(database_name, table_name)
        if permissions:
            print("Permissions:")
            for perm in permissions:
                print(f"- Principal: {perm['Principal']}, Permissions: {perm['Permissions']}")
        else:
            print("No permissions found.")

# Gọi hàm chính
if __name__ == "__main__":
    list_tables_with_details(database_name)
