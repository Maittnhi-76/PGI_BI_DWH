import smtplib
import imaplib
from email.mime.text import MIMEText

# Thông tin tài khoản
EMAIL = 'maittnhi76@gmail.com'
PASSWORD = 'ftzo odkc byjd nvdx'  # Sử dụng mật khẩu ứng dụng nếu có bật 2FA

# Kết nối IMAP
def connect_imap():
    imap = imaplib.IMAP4_SSL('imap.gmail.com')
    imap.login(EMAIL, PASSWORD)
    return imap

# Gửi email
def send_email(subject, body, to_email):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL
    msg['To'] = to_email

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(EMAIL, PASSWORD)
        server.sendmail(EMAIL, to_email, msg.as_string())

# Sử dụng hàm
if __name__ == "__main__":
    # Kết nối IMAP (không cần sử dụng trong ví dụ này, nhưng có thể dùng để kiểm tra)
    imap = connect_imap()
    imap.select('inbox')  # Chọn hộp thư đến

    # Gửi email test
    send_email('Test Email', 'This is a test email from Python!', 'nhi.mai@pgi.com.vn')

    # Đóng kết nối IMAP
    imap.logout()
