import os
import time
import pandas as pd
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
import gspread as gs

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/Repo/PGI_BI_DWH/credential/pgibidwh.json'
client = bigquery.Client()

# Google Sheets setup using Service Account credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('D:/Repo/PGI_BI_DWH/credential/pgibidwh.json', scope)
client_ggs = gs.authorize(creds)

# Open the Google Sheet by its ID and get the first worksheet
sheet = client_ggs.open_by_key('11oM3bqQFwZ5YEBQzztVaLXsK6t1VqOKnpNI2rWT0irM')
worksheet = sheet.get_worksheet(0)

# Fetch all records and load into a pandas DataFrame
records = worksheet.get_all_records()
df = pd.DataFrame(records)

# Type conversion
df = df.astype({
    'machamcong': 'str',
    'tennhanvien': 'str',
    'tenchamcong': 'str',
    'emailcongty': 'str',
    'gmailcanhan': 'str',
    'phongban': 'str'
})

# Define BigQuery table schema
table_id = 'pgibidwh.Human_Resources.dm_staff_info'
schema = [
    bigquery.SchemaField("machamcong", "STRING"),
    bigquery.SchemaField("tennhanvien", "STRING"),
    bigquery.SchemaField("tenchamcong", "STRING"),
    bigquery.SchemaField("emailcongty", "STRING"),
    bigquery.SchemaField("gmailcanhan", "STRING"),
    bigquery.SchemaField("phongban", "STRING")
]

# Configure the BigQuery load job
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition='WRITE_TRUNCATE'
)

# Load the DataFrame into BigQuery
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

# Wait for job completion using job.result() instead of a manual while loop
job.result()  # This blocks until the job is done

print(f"Job {job.job_id} completed successfully.")
