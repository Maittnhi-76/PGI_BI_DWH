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
worksheet = sheet.worksheet('dm_customers')
# Lấy tất cả các bản ghi từ trang tính
records = worksheet.get_all_records()

df = pd.DataFrame(records)
# print(df.dtypes)
df.columns = df.columns.str.strip()
df = df.astype({
    'RtlS1_CodeSub': 'str',
    'RtlInf_Type': 'str',
    'RtlInf_Channel': 'str',
    'RtlInf_Group': 'str',
    'RtlInf_Key': 'str',  
    'updated_time': 'datetime64[ns]',
    'check_unique': 'int'    
})

# Define BigQuery table schema
table_id = 'pgibidwh.Sales.dm_customers'
schema = [
    bigquery.SchemaField("RtlS1_CodeSub", "STRING"),
    bigquery.SchemaField("RtlInf_Type", "STRING"),
    bigquery.SchemaField("RtlInf_Channel", "STRING"),
    bigquery.SchemaField("RtlInf_Group", "STRING"),
    bigquery.SchemaField("RtlInf_Key", "STRING"),
    bigquery.SchemaField("updated_time", "DATE"),
    bigquery.SchemaField("check_unique", "INTEGER")   
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

