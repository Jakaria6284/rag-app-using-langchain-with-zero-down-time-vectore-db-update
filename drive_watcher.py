from googleapiclient.discovery import build
from google.oauth2 import service_account
from kafka import KafkaProducer
import json
import time
from config.settings import *

# Setup Google Drive
creds = service_account.Credentials.from_service_account_file('credentials.json')
drive_service = build('drive', 'v3', credentials=creds)

# Setup Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def watch_drive_folder():
    """Check for new files in Google Drive folder and send to Kafka"""
    print("[Drive Watcher] Starting...")
    processed_files = set()
    
    # Get initial files
    results = drive_service.files().list(
        q=f"'{GOOGLE_FOLDER_ID}' in parents",
        fields="files(id, name)"
    ).execute()
    initial_files = results.get('files', [])
    processed_files.update([f['id'] for f in initial_files])
    
    while True:
        try:
            results = drive_service.files().list(
                q=f"'{GOOGLE_FOLDER_ID}' in parents",
                fields="files(id, name)"
            ).execute()
            
            files = results.get('files', [])
            new_files = [f for f in files if f['id'] not in processed_files]
            
            if new_files:
                print(f"[Drive] Found {len(new_files)} new files")
                
                # Send individual file updates
                for file in new_files:
                    print(f"[Drive] New file: {file['name']}")
                    producer.send(EMBED_TOPIC, {
                        'type': 'file',
                        'file_id': file['id'],
                        'file_name': file['name']
                    })
                    processed_files.add(file['id'])
                
                # Send full update command
                producer.send(EMBED_TOPIC, {
                    'type': 'full_update',
                    'file_count': len(files)
                })
            
            time.sleep(30)  # Check every 30 seconds
        except Exception as e:
            print(f"[Drive Error] {e}")
            time.sleep(60)

if __name__ == "__main__":
    watch_drive_folder()