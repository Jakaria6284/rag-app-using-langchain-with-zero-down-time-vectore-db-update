from kafka import KafkaConsumer
import json
from langchain_community.document_loaders import CSVLoader
from langchain_community.vectorstores import Qdrant
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_text_splitters import CharacterTextSplitter
from qdrant_client import QdrantClient
import tempfile
import os
from config.settings import *
from qdrant_utils import get_inactive_collection, ensure_collection_exists, switch_collections
import traceback

# Setup
embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)
embedding_dim = 384  # Dimension for all-MiniLM-L6-v2

# Setup Kafka
consumer = KafkaConsumer(
    EMBED_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='vector-consumer-group'
)

print("[Vector Consumer] Ready to process files...")

def process_file(file_id, file_name, target_collection):
    """Process a file into the specified collection"""
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google.oauth2 import service_account
    
    file_path = None
    try:
        print(f"\n[Processing] File: {file_name} -> {target_collection}")
        
        # Ensure collection exists
        if not ensure_collection_exists(target_collection, embedding_dim):
            print(f"[Error] Collection {target_collection} not available")
            return
        
        # Download file
        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=file_id)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            downloader = MediaIoBaseDownload(tmpfile, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            file_path = tmpfile.name
        
        # Load and split CSV
        loader = CSVLoader(file_path=file_path)
        docs = loader.load()
        
        text_splitter = CharacterTextSplitter(
            chunk_size=1000, chunk_overlap=200)
        chunks = text_splitter.split_documents(docs)
        
        # Store in Qdrant
        Qdrant.from_documents(
            chunks,
            embeddings,
            url=QDRANT_URL,
            collection_name=target_collection,
            force_recreate=False
        )
        print(f"[Qdrant] Stored {len(chunks)} chunks in {target_collection}")
        
    except Exception as e:
        print(f"[Error] Failed to process {file_name}: {str(e)}")
        traceback.print_exc()
    finally:
        if file_path and os.path.exists(file_path):
            os.unlink(file_path)

def process_full_update():
    """Process a full dataset update with zero downtime"""
    try:
        # Get target collection (inactive)
        target_collection = get_inactive_collection()
        print(f"[Full Update] Starting on {target_collection}")
        
        # Get all files from Drive
        from googleapiclient.discovery import build
        from google.oauth2 import service_account
        
        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q=f"'{GOOGLE_FOLDER_ID}' in parents",
            fields="files(id, name)"
        ).execute()
        files = results.get('files', [])
        
        print(f"[Full Update] Processing {len(files)} files")
        
        # Process all files to target collection
        for file in files:
            process_file(file['id'], file['name'], target_collection)
        
        # Switch active collection
        new_active = switch_collections()
        print(f"[Full Update] Completed! Switched to {new_active} collection")
        
        return True
    except Exception as e:
        print(f"[Full Update Error] {str(e)}")
        traceback.print_exc()
        return False

# Main loop
for msg in consumer:
    data = msg.value
    try:
        if data['type'] == 'file':
            target_collection = get_inactive_collection()
            process_file(data['file_id'], data['file_name'], target_collection)
        elif data['type'] == 'full_update':
            process_full_update()
    except Exception as e:
        print(f"[Consumer Error] {str(e)}")
        traceback.print_exc()