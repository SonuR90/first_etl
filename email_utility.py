import smtplib
from email.message import EmailMessage
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

def send_email_native(subject, body):
    sender = "your_email@example.com"
    recipient = "recipient@example.com"
    password = config["gmail_app_password"]["pass_key"]

    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)
    print("Email Sent")
