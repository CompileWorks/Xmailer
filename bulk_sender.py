import smtplib
import json
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import threading
import csv
import logging

# Configure logging
logging.basicConfig(filename='email_sender.log', level=logging.INFO)

class BulkEmailSender:
    def __init__(self):
        self.load_config()
        self.active_connections = []
        
    def load_config(self):
        with open('smtp_config.json') as f:
            self.smtp_config = json.load(f)
            
    def connect_smtp(self, server_config):
        try:
            if server_config['port'] == 465:
                server = smtplib.SMTP_SSL(server_config['host'], server_config['port'])
            else:
                server = smtplib.SMTP(server_config['host'], server_config['port'])
                server.starttls()
                
            server.login(server_config['username'], server_config['password'])
            return server
        except Exception as e:
            logging.error(f"SMTP Connection Error: {str(e)}")
            return None
            
    def send_email(self, server, from_email, to_email, subject, body):
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))
        
        try:
            server.sendmail(from_email, to_email, msg.as_string())
            logging.info(f"Sent to {to_email}")
            return True
        except Exception as e:
            logging.error(f"Failed to send to {to_email}: {str(e)}")
            return False
            
    def process_batch(self, batch, template, server_config):
        server = self.connect_smtp(server_config)
        if not server:
            return
            
        success = 0
        for recipient in batch:
            email_body = template.format(**recipient)
            if self.send_email(server, server_config['username'], recipient['email'], 
                              recipient['subject'], email_body):
                success += 1
            # Throttle to avoid being flagged as spam
            time.sleep(0.5)
            
        server.quit()
        return success
        
    def distribute_load(self, recipients, email_template, threads=5):
        total = len(recipients)
        batch_size = total // threads
        batches = [recipients[i:i + batch_size] for i in range(0, total, batch_size)]
        
        threads = []
        for i, batch in enumerate(batches):
            server_config = self.smtp_config['smtp_servers'][i % len(self.smtp_config['smtp_servers'])]
            t = threading.Thread(target=self.process_batch, args=(batch, email_template, server_config))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()

if __name__ == "__main__":
    sender = BulkEmailSender()
    
    # Load recipients from CSV
    recipients = []
    with open('recipients.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            recipients.append(row)
    
    # Email template with placeholders
    email_template = """
    <html>
    <body>
        <p>Hello {name},</p>
        <p>{message}</p>
    </body>
    </html>
    """
    
    sender.distribute_load(recipients, email_template)
