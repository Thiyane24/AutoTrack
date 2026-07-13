import imaplib
import email
from dotenv import load_dotenv
import os
import beautifulsoup4 as bs 

load_dotenv()

EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_APP_PASSWORD = os.getenv('EMAIL_APP_PASSWORD')

with imaplib.IMAP4_SSL("imap.gmail.com") as mail:
    mail.login(EMAIL_USER, EMAIL_APP_PASSWORD)
    mail.select("Inbox")

    status, messages = mail.search(None, 'UNSEEN', 'SUBJECT', 'Intern')
    email_ids = messages[0].split()

    for email_id in email_ids:
        status, msg_data = mail.fetch(email_id, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject = msg["subject"]
                from_ = msg["from"]
                print(f"Subject: {subject}, From: {from_}")