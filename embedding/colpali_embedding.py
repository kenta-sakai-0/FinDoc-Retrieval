# uvicorn colpali_embedding:app --host 0.0.0.0 --port 8000

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from project_config import qdrant_config

import torch
from transformers import AutoProcessor
from colpali_engine.models import ColPali, ColPaliProcessor
from pdf2image import convert_from_path
from qdrant_client import QdrantClient, models
import uuid

from fastapi import FastAPI, Body
app = FastAPI()

# Config
model_name = "vidore/colpali-v1.2"
model = ColPali.from_pretrained(
    model_name,
    torch_dtype=torch.bfloat16,
    device_map="cuda:0",
).eval()
processor = ColPaliProcessor.from_pretrained(model_name)

def embed_one_image(image):
    """
    Input: image
    Output: 2-dimensional matrix 
    """
    batch_images = processor.process_images([image]).to(model.device)
    with torch.no_grad():
        # Move processed image to the same device as model
        processed_image = {
            k: (v.to(model.device, dtype=torch.bfloat16) 
                if k == 'pixel_values' 
                else v.to(model.device))
            for k, v in batch_images.items()
        }
        embedding = model(**processed_image)
    return embedding[0].tolist()

@app.post("/embed_query")
def embed_query(query: dict = Body(...)):
    query = query['query']
    q = processor.process_queries([query]).to(model.device)
    embedding = model(**q)
    return embedding[0].tolist()

@app.post("/embed_pdf")
def embed_pdf(pdf_filepath:dict = Body(...)) -> list[list]:
    pdf_filepath = pdf_filepath['pdf_filepath']
    print(f"Converting PDF at {pdf_filepath} to images...")
    images = convert_from_path(pdf_filepath, poppler_path=r"C:\Program Files\poppler-24.07.0\Library\bin")
    embeddings = []
    for page_number, image in enumerate(images):
        print(f"Processing page {page_number + 1}...")
        embeddings.append(embed_one_image(image))
    print(f"PDF conversion complete with {len(images)} pages.")
    return embeddings

@app.post("/embed_submissions")
def embed_submissions(submissions_dict:dict = Body(...))->None:
# def embed_submissions(submissions_dict)->None:
    """
    Step 4 of DAG. 
    (Submissions is a dictionary with accession number as key.)
    Convert PDF to colpali style embeddings and upload to qdrant
    """
    for accession_number, values in submissions_dict.items():
        # pdf_savepath = values['pdf_savepath']
        values.update({"accession_number": accession_number})
        pdf_savepath = rf"C:\Users\Kenta Sakai\projects\FinDoc-Retrieval\data\sec_filings\pdf\{accession_number}.pdf"
        
        images = convert_from_path(pdf_savepath, poppler_path=r"C:\Program Files\poppler-24.07.0\Library\bin")
        
        qdrant_client = QdrantClient(
            "http://localhost", 
            port=qdrant_config.QDRANT_PORT
        )

        total_pages = len(images)
   
        for i, image in enumerate(images):
            values.update({"page_number": i})
            qdrant_client.upsert(
                collection_name=qdrant_config.QDRANT_COLLECTION_NAME,
                points=[
                    models.PointStruct(
                        id=str(uuid.uuid4()),
                        payload = values,
                        vector={
                            "colpali": embed_one_image(image).tolist()
                        }
                    )
                ]
            )
            print(f"{i+1}/{total_pages} processed")

# if __name__ == "__main__":
#     test_data = {
#         "0001193125-22-104437": {}  # Empty dict as value is fine since the endpoint adds the accession number
#     }
#     embed_submissions(test_data)