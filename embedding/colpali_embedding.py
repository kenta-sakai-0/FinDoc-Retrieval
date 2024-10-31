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
device = torch.device("cuda")
model_name = "vidore/colpali"
model = ColPali.from_pretrained(
    "vidore/colpaligemma-3b-mix-448-base", 
    torch_dtype=torch.bfloat16, 
    device_map="cuda"
).eval()

model.load_adapter(model_name)
processor = AutoProcessor.from_pretrained(model_name)

def embed_one_image(image):
    """
    Input: image
    Output: 2-dimensional matrix 
    """
    processed_image = ColPaliProcessor.process_images(processor, [image])
    with torch.no_grad():
        # Move processed image to the same device as model
        processed_image = {k: v.to(device) for k, v in processed_image.items()}
        embedding = model(**processed_image)
    return embedding[0]

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
