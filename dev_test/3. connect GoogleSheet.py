import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Bật Google Sheets API và Google Drive API trong Google Cloud Console.
# Cấp quyền truy cập cho tài khoản dịch vụ vào bảng tính Google Sheets.
# Chạy lại mã sau khi thay đổi có hiệu lực (có thể cần một chút thời gian).
# Định nghĩa phạm vi quyền truy cập
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Tạo đối tượng xác thực từ tệp JSON
creds = ServiceAccountCredentials.from_json_keyfile_name('D:/Repo/PGI-BI-DWH/airflow/credential/pgibidwh.json', scope)
client_ggs = gspread.authorize(creds)
sheet = client_ggs.open_by_key('11oM3bqQFwZ5YEBQzztVaLXsK6t1VqOKnpNI2rWT0irM')
worksheet = sheet.get_worksheet(0)
data = worksheet.get_all_records()
print("Dữ liệu từ bảng tính:")
for row in data:
    print(row)

# # Kết nối với Google Sheets
# try:
#     client_ggs = gspread.authorize(creds)
#     print("Kết nối thành công!")
# except Exception as e:
#     print("Lỗi khi kết nối:", e)
#     exit()

# # Mở bảng tính theo key
# try:
#     sheet = client_ggs.open_by_key('1wZ62mY7jvQLEkeWWAoZb_U60mRUrCFSxf0e7FO81q_c')
#     # sheet = client_ggs.open('TARGET')
#     print("Mở bảng tính thành công!")
# except gspread.exceptions.APIError as api_error:
#     print("Lỗi API:", api_error.response)  # In phản hồi chi tiết của API
# except gspread.exceptions.SpreadsheetNotFound:
#     print("Không tìm thấy bảng tính với key đã cho.")
# except gspread.exceptions.GSpreadException as e:
#     print("Lỗi GSpread:", str(e))
# except Exception as e:
#     print("Lỗi không xác định:", str(e))

# try:
#     worksheet = sheet.get_worksheet(0)
#     print("Lấy worksheet thành công!")
# except Exception as e:
#     print("Lỗi khi lấy worksheet:", e)
#     exit()

# # Lấy worksheet đầu tiên
# # worksheet = sheet.get_worksheet(0)

# # Lấy dữ liệu và in ra
# data = worksheet.get_all_records()
# print("Dữ liệu từ bảng tính:")
# for row in data:
#     print(row)