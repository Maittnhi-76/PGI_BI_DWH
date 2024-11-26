import sys
import os
import time
import pymssql
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery


#------------------------------Định nghĩa function-----------------
#Hàm để đọc sql từ path
def execute_sql_from_file(sql_file_path):
    """Đọc truy vấn từ tệp .sql"""
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        query = file.read()
    return query

# Hàm để lấy dữ liệu từ SQL Server
def get_data_from_sql_server(var_server, var_user, var_password, var_database, var_port, var_sqlinput):
    try:
        # Lấy thông tin kết nối từ biến môi trường
        server = os.getenv('SQL_SERVER', var_server)
        port = os.getenv('SQL_PORT', var_port)
        database = os.getenv('SQL_DATABASE', var_database)
        username = os.getenv('SQL_USER', var_user)
        password = os.getenv('SQL_PASSWORD', var_password)
        # Tạo chuỗi kết nối
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
        
        # Kết nối đến cơ sở dữ liệu với `with` statement
        with pyodbc.connect(connection_string) as conn:
            # Truy vấn dữ liệu
            sql_query = execute_sql_from_file(var_sqlinput)
            # Đọc dữ liệu vào DataFrame
            datafarm_outcome = pd.read_sql(sql_query, conn)
            
            # Kiểm tra xem có dữ liệu không
            if not datafarm_outcome.empty:
                # Thêm cột ngày hiện tại
                datafarm_outcome['current_date'] = datetime.now().date()

                # In dữ liệu ra
                # print("Dữ liệu lấy từ SQL Server thành công!")
                # print(f"Dữ liệu:\n{datafarm_outcome}")
                return datafarm_outcome
            else:
                print("Không có dữ liệu nào được trả về từ SQL Server.")
                
    except Exception as e:
        print(f"Lỗi kết nối hoặc truy vấn: {e}")
    
    # return datafarm_outcome
# Hàm để tải dữ liệu lên BigQuery
def load_data_to_bigquery(var_dataframe, var_table_id, var_write_disposition = 'WRITE_TRUNCATE'):
    # Cấu hình đường dẫn cho credentials của BigQuery
    # credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/Repo/PGI-BI-DWH/airflow/credential/pgibidwh.json'
   # Kết nối đến BigQuery và tải dữ liệu vào bảng
    client = bigquery.Client()
    # table_id = var_table_id

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition= var_write_disposition
    )

    job = client.load_table_from_dataframe(var_dataframe, var_table_id, job_config=job_config)

    # Đợi cho công việc tải dữ liệu hoàn thành
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()

    # In kết quả của công việc tải
    return print(job.result())

def get_data_from_bigquery(var_pathinputsql):
    # BQ credential
    # credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/Repo/PGI-BI-DWH/airflow/credential/pgibidwh.json'
    client = bigquery.Client()

    # lấy ngày trong tháng từ Bigquery
    inputsql = execute_sql_from_file(var_pathinputsql)
    # inputsql = var_pathinputsql
    result = client.query(inputsql).result().to_dataframe()
    return result

#----------------------Dùng function------------------------------------------------------------------------


def get_and_load_data():
    var_sqlinput='D:/Repo/PGI-BI-DWH/python/sql/hr_check_in_out.sql'
    var_server='43.239.220.33'
    var_user='ba01'
    var_password='123456aA@'
    var_database='MITACOSQL'
    var_port=1433
    
    var_dataframe=get_data_from_sql_server(var_server, var_user, var_password, var_database, var_port, var_sqlinput)
    # var_dataframe=get_data_from_sql_server()
    var_table_id='pgibidwh.Human_Resources.checkin_checkout_detail'
    load_data_to_bigquery(var_dataframe, var_table_id, var_write_disposition = 'WRITE_TRUNCATE')

get_and_load_data()