import torch
from transformers import AutoProcessor
from colpali_engine.models import ColPali, ColPaliProcessor
from pdf2image import convert_from_path
import torch

# Config
device = torch.device("cuda")
model_name = "vidore/colpali"
model = ColPali.from_pretrained(
    "vidore/colpaligemma-3b-mix-448-base", 
    torch_dtype=torch.bfloat16, 
    device_map="auto"  # This will use GPU if available
).eval()

model.load_adapter(model_name)
processor = AutoProcessor.from_pretrained(model_name)

def embed_pdf(pdf_path):
    """
    Takes a pdf_path and generates tensor
    """
    images = convert_from_path(pdf_path, poppler_path=r"C:\Program Files\poppler-24.07.0\Library\bin")

    num_pages = len(images)
    tensor = []
    for i, image in enumerate(images):
        tensor.append(embed_one_image(image))
        print(f"{i+1}/{num_pages} processed")

    embeddings = torch.stack(tensor)
    print(embeddings.size())
    return embeddings

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