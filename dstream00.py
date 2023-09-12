import os

# Đường dẫn đến thư mục "input"
folder_path = "input"

# Kiểm tra xem thư mục có tồn tại không
if os.path.exists(folder_path):
    # Lấy danh sách tệp tin trong thư mục
    file_list = os.listdir(folder_path)

    # Kiểm tra xem thư mục có rỗng hay không
    if len(file_list) == 0:
        print("Thư mục rỗng")
    else:
        for file_name in file_list:
            # Tạo đường dẫn đến tệp tin
            file_path = os.path.join(folder_path, file_name)

            # Kiểm tra xem file_path là thư mục hay là tệp tin
            if os.path.isfile(file_path):
                print("Nội dung của", file_name, "là:")

                # Đọc và in nội dung của tệp tin
                with open(file_path, 'r') as file:
                    content = file.read()
                    print(content)
                print("\n")
else:
    print("Thư mục không tồn tại")
