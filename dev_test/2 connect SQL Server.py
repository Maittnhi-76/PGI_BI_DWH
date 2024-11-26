import pyodbc
import pandas as pd
from datetime import datetime
import os

def execute_sql_from_file(sql_file_path):
    """Đọc truy vấn từ tệp .sql"""
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        query = file.read()
    return query

def get_data_from_sql_server():
    try:
        # Lấy thông tin kết nối từ biến môi trường
        server = os.getenv('SQL_SERVER', '43.239.220.33')
        port = os.getenv('SQL_PORT', '1433')
        database = os.getenv('SQL_DATABASE', 'MITACOSQL')
        username = os.getenv('SQL_USER', 'ba01')
        password = os.getenv('SQL_PASSWORD', '123456aA@')

        # Tạo chuỗi kết nối
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
        
        # Kết nối đến cơ sở dữ liệu với `with` statement
        with pyodbc.connect(connection_string) as conn:
            # Truy vấn dữ liệu
            sql_query = """
                SELECT 
                    t1.machamcong,
                    tennhanvien,
                    tenchamcong,
                    ngaycham,
                    MIN(GIOCHAM) AS checkin,
                    CASE WHEN MAX(giocham) = MIN(giocham) THEN NULL ELSE MAX(giocham) END AS checkout
                FROM CHECKINOUT T1
                LEFT JOIN NHANVIEN T2 ON T1.MaChamCong = T2.MaChamCong 
                WHERE ngaycham >= '2024-11-01' AND ngaycham < '2024-11-30'
                GROUP BY t1.machamcong, tennhanvien, tenchamcong, NGAYCHAM
                ORDER BY ngaycham
            """
            # Đọc dữ liệu vào DataFrame
            datafarm_outcome = pd.read_sql(sql_query, conn)
            
            # Kiểm tra xem có dữ liệu không
            if not datafarm_outcome.empty:
                # Thêm cột ngày hiện tại
                datafarm_outcome['current_date'] = datetime.now().date()

                # In dữ liệu ra
                print("Dữ liệu lấy từ SQL Server thành công!")
                print(f"Dữ liệu:\n{datafarm_outcome}")
            else:
                print("Không có dữ liệu nào được trả về từ SQL Server.")
                
    except Exception as e:
        print(f"Lỗi kết nối hoặc truy vấn: {e}")

# Gọi hàm để thực hiện lấy dữ liệu
get_data_from_sql_server()
