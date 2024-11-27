from datetime import timedelta, datetime
import pandas as pd
import sys
import time
from google.cloud import bigquery
import os
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

#-----------------------General Information----------------------------------
#BQ credential
#Set var in local and cloud is difference
# credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/Repo/PGI_BI_DWH/credential/pgibidwh.json'
client = bigquery.Client()

# Xác định phạm vi của quyền truy cập
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# Đường dẫn tới tệp JSON của tài khoản dịch vụ
creds = ServiceAccountCredentials.from_json_keyfile_name('D:/Repo/PGI_BI_DWH/credential/pgibidwh.json', scope)
# Uỷ quyền với các thông tin từ tệp JSON
client_ggs = gs.authorize(creds)
# Mở Google Sheet bằng ID
sheet = client_ggs.open_by_key('1wZ62mY7jvQLEkeWWAoZb_U60mRUrCFSxf0e7FO81q_c')
# Lấy trang tính đầu tiên
worksheet = sheet.worksheet('TARGET')
# Lấy tất cả các bản ghi từ trang tính
records = worksheet.get_all_records()

df = pd.DataFrame(records)
df.columns = df.columns.str.strip()
df = df.astype({
    'DlrCode': 'int',
    'DlrS1Name': 'str',
    'DlrSCode': 'int',
    'DlrS1Name1': 'str',
    'target': 'int',
    'DlrTarget': 'int',
    'Slman': 'str',
    'slmanB1': 'str',
    'Namemap': 'str',
    'yearmonth': 'datetime64[ns]'
   
})

# Define BigQuery table schema
table_id = 'pgibidwh.Sales.target'
schema = [
    bigquery.SchemaField("DlrCode", "INTEGER"),
    bigquery.SchemaField("DlrS1Name", "STRING"),
    bigquery.SchemaField("DlrSCode", "INTEGER"),
    bigquery.SchemaField("DlrS1Name1", "STRING"),
    bigquery.SchemaField("target", "INTEGER"),
    bigquery.SchemaField("DlrTarget", "INTEGER"),
    bigquery.SchemaField("Slman", "STRING"),
    bigquery.SchemaField("slmanB1", "STRING"),
    bigquery.SchemaField("Namemap", "STRING"),
    bigquery.SchemaField("yearmonth", "DATE")
]

job_config= bigquery.LoadJobConfig(
# autodetect= True,
schema=schema,
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(df,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())

