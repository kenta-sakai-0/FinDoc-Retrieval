import requests
import pandas as pd 
from dotenv import load_dotenv
import os
import requests
from sec_downloader import Downloader
from weasyprint import HTML

load_dotenv()

def get_missing_pdf():
    # Load my_filings
    my_filings_savepath = os.getenv("MY_FILINGS_SAVEPATH")
    my_filings = pd.read_csv(my_filings_savepath)
    
    # Make list of filings I already have
    pdf_folder_path = os.getenv("PDF_FOLDER_PATH")
    existingPDFs = os.listdir(pdf_folder_path)

    # Identify missing filings
    missing_my_filings = my_filings[~(my_filings['accessionNumber'] + ".pdf").isin(existingPDFs)]
    
    num_processed, num_missing = 0, len(missing_my_filings)
    for index, row in missing_my_filings.iterrows():

        download_pdf(row["CIK"], row["accessionNumber"])
        num_processed+=1

        print(f"Saved {num_processed}/{num_missing} documents")
        
def download_pdf(
    # Download EDGAR filing as PDF using CIK & accessionNumber
        CIK: str,
        accessionNumber: str,
        pdf_folder_path = os.getenv("PDF_FOLDER_PATH")
        ):
    
    EDGAR_HEADER = os.getenv("EDGAR_HEADER")

    # Find filing URL
    dl = Downloader("User-Agent", EDGAR_HEADER)
    metadatas = dl.get_filing_metadatas(f"{CIK}/{accessionNumber}")
    primary_doc_url = metadatas[0].primary_doc_url
    
    # Get HTML string using primary_doc_url
    html = requests.get(primary_doc_url, headers={"User-Agent": EDGAR_HEADER}).text

    # Convert HTML string into PDF and save to pdf_destination_filepath
    pdf_savepath = pdf_folder_path + "/" + accessionNumber + ".pdf"
    htmldoc = HTML(string=html).write_pdf(pdf_savepath)