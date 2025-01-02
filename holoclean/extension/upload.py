import shutil
import os
import requests

def upload_file_to_nestjs_api(file_path, bucket_name):
    url = 'http://localhost:3000/upload'  # Địa chỉ API của bạn
    params = {
        'folderPath': file_path,
        'bucketName': bucket_name,
    }
    # Gửi yêu cầu GET với các tham số
    response = requests.get(url, params=params)

    print(response)

    # Kiểm tra xem có lỗi không
    if response.status_code == 200:
        print("Files uploaded successfully.")
    else:
        print(f"Error: {response.status_code} - {response.text}")


def clear_output_folder(folder_path):
    """
    Deletes all files and folders inside the given folder.

    :param folder_path: Path to the folder to be cleared.
    """
    try:
        if os.path.exists(folder_path):
            # Xóa tất cả file và folder con trong folder chỉ định
            for file_or_folder in os.listdir(folder_path):
                path = os.path.join(folder_path, file_or_folder)
                if os.path.isfile(path) or os.path.islink(path):
                    os.unlink(path)  # Xóa file hoặc symbolic link
                elif os.path.isdir(path):
                    shutil.rmtree(path)  # Xóa folder và nội dung bên trong
            print(f"All contents of '{folder_path}' have been deleted.")
        else:
            print(f"The folder '{folder_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred while clearing the folder: {e}")
