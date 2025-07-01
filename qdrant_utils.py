from qdrant_client import QdrantClient
from qdrant_client.http import models
from config.settings import *
import traceback

qdrant = QdrantClient(QDRANT_URL)

def initialize_control_collection():
    """Create control collection if it doesn't exist"""
    try:
        collections = qdrant.get_collections()
        collection_names = [col.name for col in collections.collections]
        
        if ACTIVE_COLLECTION_CONTROL not in collection_names:
            print(f"[Qdrant] Creating control collection: {ACTIVE_COLLECTION_CONTROL}")
            qdrant.create_collection(
                collection_name=ACTIVE_COLLECTION_CONTROL,
                vectors_config=models.VectorParams(size=1, distance=models.Distance.COSINE)
            )
            
            # Initialize with blue collection
            qdrant.upsert(
                collection_name=ACTIVE_COLLECTION_CONTROL,
                points=[models.PointStruct(id=1, vector=[0.0], payload={"active_collection": COLLECTION_BLUE})]
            )
            return COLLECTION_BLUE
    except Exception as e:
        print(f"[Qdrant Error] Control collection init failed: {e}")
        traceback.print_exc()
    return None

def get_active_collection():
    """Get the currently active collection (blue or green)"""
    try:
        # Initialize control collection if needed
        initialize_control_collection()
        
        # Get active collection
        result = qdrant.scroll(
            collection_name=ACTIVE_COLLECTION_CONTROL,
            limit=1,
            with_payload=True
        )
        
        if result and result[0]:
            point = result[0][0]
            return point.payload.get('active_collection', COLLECTION_BLUE)
    except Exception as e:
        print(f"[Qdrant Error] Failed to get active collection: {e}")
        traceback.print_exc()
    
    # Fallback to default
    return COLLECTION_BLUE

def set_active_collection(collection_name):
    """Set the active collection (blue or green)"""
    try:
        # Update active collection
        qdrant.upsert(
            collection_name=ACTIVE_COLLECTION_CONTROL,
            points=[models.PointStruct(id=1, vector=[0.0], payload={"active_collection": collection_name})]
        )
        print(f"[Qdrant] Active collection set to: {collection_name}")
        return True
    except Exception as e:
        print(f"[Qdrant Error] Failed to set active collection: {e}")
        traceback.print_exc()
        return False

def get_inactive_collection():
    """Get the inactive collection for updates"""
    active = get_active_collection()
    return COLLECTION_GREEN if active == COLLECTION_BLUE else COLLECTION_BLUE

def switch_collections():
    """Switch between blue and green collections"""
    active = get_active_collection()
    new_active = COLLECTION_GREEN if active == COLLECTION_BLUE else COLLECTION_BLUE
    set_active_collection(new_active)
    return new_active

def ensure_collection_exists(collection_name, embedding_dim):
    """Ensure a collection exists with proper configuration"""
    try:
        collections = qdrant.get_collections()
        collection_names = [col.name for col in collections.collections]
        
        if collection_name not in collection_names:
            print(f"[Qdrant] Creating collection: {collection_name}")
            qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=embedding_dim,
                    distance=models.Distance.COSINE
                )
            )
            return True
        else:
            print(f"[Qdrant] Collection {collection_name} exists")
            return True
    except Exception as e:
        print(f"[Qdrant Error] Collection creation failed: {e}")
        traceback.print_exc()
        return False