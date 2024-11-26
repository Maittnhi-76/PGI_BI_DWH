# pip install --upgrade google-cloud-bigquery     
import os
from google.cloud import bigquery
# Thiết lập biến môi trường cho xác thực
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'Airflow/credential/dw-pgi-5581e99467ae.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/Repo/PGI-BI-DWH/airflow/credential/pgibidwh.json'

# Tạo client BigQuery
client = bigquery.Client()

# Thực hiện truy vấn
# query = """
#           SELECT DlrS1Name1,target,DlrTarget,Slman, slmanB1,Namemap,yearmonth 
#           FROM `dw-pgi.Sales.Target` 
#           LIMIT 1000
#         """
query = """
          SELECT *
          FROM pgibidwh.Human_Resources.checkin_checkout_detail
          LIMIT 1000
        """
query_job = client.query(query)

# Nhận kết quả
for row in query_job.result():
    print(row)     

     