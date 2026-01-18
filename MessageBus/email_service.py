# -*- coding:utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

class EmailService:
    """
    Message Bus Layer - Email Service
    """
    
    def __init__(self, smtp_server, smtp_port, sender_email, sender_password, receiver_email):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.receiver_email = receiver_email
        
    def send_email(self, subject, body, attachment_files=None):
        if not self.sender_email or not self.receiver_email:
            print("⚠️ 邮件发送跳过: 未配置发件人或收件人")
            return False
            
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = self.receiver_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            if attachment_files:
                for file_path in attachment_files:
                    if os.path.exists(file_path):
                        with open(file_path, 'rb') as f:
                            part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
                        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                        msg.attach(part)
                    else:
                        print(f"⚠️ 附件不存在: {file_path}")
            
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
                
            print(f"📧 邮件已发送至 {self.receiver_email}")
            return True
        except Exception as e:
            print(f"❌ 邮件发送失败: {e}")
            return False
