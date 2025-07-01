from langchain_community.vectorstores import Qdrant
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_groq import ChatGroq
from langchain.chains import RetrievalQA
from qdrant_client import QdrantClient
from config.settings import *
from qdrant_utils import get_active_collection
import traceback

# Setup clients
qdrant_client = QdrantClient(QDRANT_URL)
embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)

def get_answer(query):
    """Get answer for user query"""
    try:
        # Get active collection
        active_collection = get_active_collection()
        print(f"[LLM] Using collection: {active_collection}")
        
        # Create vector store connection
        vector_store = Qdrant(
            client=qdrant_client,
            collection_name=active_collection,
            embeddings=embeddings
        )
        
        # Setup LLM
        llm = ChatGroq(
            groq_api_key=GROQ_API_KEY,
            model_name="qwen/qwen3-32b",
            temperature=0.1
        )
        
        # Create QA chain
        qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=vector_store.as_retriever(search_kwargs={"k": 3}),
            return_source_documents=False
        )
        
        # Get response
        response = qa_chain.invoke({"query": query})
        return response["result"]
        
    except Exception as e:
        traceback.print_exc()
        return f"Error: {str(e)}"