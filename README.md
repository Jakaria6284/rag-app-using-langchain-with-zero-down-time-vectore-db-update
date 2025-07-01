# Customer Support Bot with Zero-Downtime Knowledge Updates



This project implements an intelligent customer support bot that automatically updates its knowledge base from CSV files in Google Drive and provides instant answers to user queries using Groq's fast LLMs. The system features zero-downtime updates through blue/green deployment with Qdrant vector database.

---

## 🚀 Key Features

- **Automatic Knowledge Updates**: Monitors Google Drive for new CSV files
- **Zero-Downtime Updates**: Blue/green deployment pattern for seamless updates
- **Fast Response Times**: Uses Groq's ultra-fast LLMs for instant answers
- **Scalable Architecture**: Kafka-based message queue for parallel processing
- **Atomic Updates**: Background processing with instant switch to new knowledge
- **Version Control**: Maintains two complete versions of knowledge for easy rollback

---
## ✅ Prerequisites
Before you begin, ensure you have the following installed:

- **Python 3.9+**
- **Kafka** (via docker means need to install docker dekstop)
- **Qdrant vector database** (via Docker or standalone)
- **Google Service Account credentials**
- **Groq API Key** (from Groq Cloud)

  ## ⚙️ Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/yourusername/customer-support-bot.git
   cd customer-support-bot
2. **make virtua env ,activate and Install Dependencies**

   ```bash
   python -m venv venv 

   # Activate virtual environment
   # For Linux/Mac:
   source venv/bin/activate

   # For Windows:
   venv\Scripts\activate

   pip install -r requirements.txt
3. **Configure Environment**

   Create a `config/settings.py` file under the root directory  and add the following:

   ```python
   # Google Drive Configuration
   GOOGLE_FOLDER_ID = "your-google-drive-folder-id"

   # Qdrant Configuration
   QDRANT_URL = "http://localhost:6333"

   # Embedding Model
   EMBEDDING_MODEL = "all-MiniLM-L6-v2"

   # Kafka Configuration
   KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
   EMBED_TOPIC = "drive_csv"
   QUERY_TOPIC = "query_topic"

   # Groq API
   GROQ_API_KEY = "your-groq-api-key"

   # Qdrant Collections
   COLLECTION_BLUE = "support_data_blue"
   COLLECTION_GREEN = "support_data_green"
   ACTIVE_COLLECTION_CONTROL = "active_collection_control"


4. **Google Service Account Setup**

   - Create a service account in **Google Cloud Console**
   - Download the **credentials JSON file**
   - Save it as `credentials.json` in the project root
   - Share your **Google Drive folder** with the service account email
5. **Start runing project**

   ```bash
   docker-compose up -d
   
## Project structure
```plaintext
customer-support-bot/
├── config/
│   └── settings.py       # Configuration settings
├── credentials.json      # Google service account credentials
├── drive_watcher.py      # Monitors Google Drive for changes
├── llm_chain.py          # Question answering with LLM
├── qdrant_utils.py       # Vector DB utilities
├── query_consumer.py     # Answers user queries
├── query_producer.py     # User interface for asking questions
├── vector_consumer.py    # Processes files into vector DB
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation

  








## 🚀 Running the System

  Open four separate terminal windows and run the following commands respectively:

  ```bash
  # Terminal 1: Drive Watcher
  python drive_watcher.py

  # Terminal 2: Vector Consumer
  python vector_consumer.py

  # Terminal 3: Query Consumer
  python query_consumer.py

  # Terminal 4: Query Producer (User Interface)
  python query_producer.py



