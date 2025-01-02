import requests 
from qdrant_client import QdrantClient, models

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from project_config import qdrant_config

def search(query):
    query = embed_query(query)

    qdrant_client = QdrantClient(
        "http://localhost", 
        port=qdrant_config.QDRANT_PORT
        )
    
    query_results = qdrant_client.query_points(
        collection_name=qdrant_config.QDRANT_COLLECTION_NAME,
        query=query,
        using="colpali",
        limit = 3
    )
    
    return query_results

def embed_query(query:str):
    url = "http://localhost:8000/embed_query"
    response = requests.post(url, json={"query": query})
    return response.json()

if __name__ == "__main__":
    text = "What was LSF's revenue in Q3 2021?"
    print(search(text))