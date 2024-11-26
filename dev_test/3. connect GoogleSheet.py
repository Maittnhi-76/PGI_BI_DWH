import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Hàm xác thực tài khoản dịch vụ và trả về client để truy cập Google Sheets.
def authenticate_google_sheets(credentials_file, scope):
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Error during authentication: {e}")
        return None

# Hàm lấy dữ liệu từ bảng tính Google Sheets theo ID và chỉ định worksheet.
def fetch_sheet_data(client, spreadsheet_id, worksheet_index=0):
    try:
        sheet = client.open_by_key(spreadsheet_id)
        worksheet = sheet.get_worksheet(worksheet_index)
        data = worksheet.get_all_records()
        return data
    except Exception as e:
        print(f"Error while fetching data: {e}")
        return []

def main():
    # Định nghĩa phạm vi quyền truy cập
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Đường dẫn đến tệp chứng thực JSON
    credentials_file = 'D:/Repo/PGI_BI_DWH/credential/pgibidwh.json'
    # ID của bảng tính cần truy cập
    spreadsheet_id = '11oM3bqQFwZ5YEBQzztVaLXsK6t1VqOKnpNI2rWT0irM'
    # Xác thực và lấy client
    client = authenticate_google_sheets(credentials_file, scope)
    if client:
        # Lấy dữ liệu từ bảng tính
        data = fetch_sheet_data(client, spreadsheet_id)        
        if data:
            print("Dữ liệu từ bảng tính:")
            for row in data:
                print(row)
        else:
            print("Không có dữ liệu từ bảng tính.")
    else:
        print("Không thể xác thực với Google Sheets API.")

if __name__ == "__main__":
    main()
