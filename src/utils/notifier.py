import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

class Notifier:
    def __init__(self, config_manager, message_bus):
        self.cfg = config_manager
        self.bus = message_bus
    
    def send_email(self, subject, body, attachment_files=[]):
        """
        Send email with attachments.
        """
        email_cfg = self.cfg.EMAIL
        
        if not email_cfg.get("enable", False):
            self.bus.publish("INFO", "Email disabled, skipping.")
            return

        sender = email_cfg.get("sender_email")
        password = email_cfg.get("sender_password")
        receiver = email_cfg.get("receiver_email")
        smtp_server = email_cfg.get("smtp_server", "smtp.qq.com")
        smtp_port = email_cfg.get("smtp_port", 465)

        if not sender or not password:
            self.bus.publish("ERROR", "Missing EMAIL_SENDER or EMAIL_PASSWORD env vars")
            return

        try:
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = receiver
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            for file_path in attachment_files:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as attachment:
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                    msg.attach(part)
                else:
                    self.bus.publish("WARNING", f"Attachment not found: {file_path}")

            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
            server.quit()
            
            self.bus.publish("INFO", f"Email sent successfully to {receiver}")
            
        except Exception as e:
            self.bus.publish("ERROR", f"Failed to send email: {e}")
