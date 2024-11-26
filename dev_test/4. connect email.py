import smtplib
import ssl
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(email_from, password, email_to, subject, body):
    """Gửi email sử dụng Gmail."""
    # Tạo một đối tượng MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = email_to
    msg['Subject'] = subject

    # Đính kèm nội dung email
    msg.attach(MIMEText(body, "html"))

    # Thiết lập kết nối SSL và gửi email
    # context = ssl.create_default_context()
    # with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
    #     server.login(email_from, password)  # Đăng nhập vào tài khoản Gmail
    #     server.sendmail(email_from, email_to, msg.as_string())  # Gửi email


# Gọi hàm gửi email
# send_email(email_from, password, email_to, subject, body)


def send_email(email_from, app_password, email_to, subject, body):
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
            server.login(email_from, app_password)  # Đăng nhập vào tài khoản Gmail
            email_message = MIMEMultipart()
            email_message['From'] = email_from
            email_message['To'] = email_to
            email_message['Subject'] = subject
            email_message.attach(MIMEText(body, "html"))
            server.sendmail(email_from, email_to, email_message.as_string())
            print("Email đã được gửi thành công!")
    except Exception as e:
        print("Lỗi khi gửi email:", str(e))

# Thay thế thông tin dưới đây bằng thông tin của bạn
email_from = 'maittnhi76@gmail.com'
password = 'ftzo odkc byjd nvdx'  # Sử dụng mật khẩu ứng dụng nếu có bật 2FA
email_to = 'nhi.mai@pgi.com.vn'
subject = 'Test Email from Python'
body = '<h1>This is a test email</h1><p>Hello from Python!</p>'


send_email(email_from, password, email_to, subject, body)