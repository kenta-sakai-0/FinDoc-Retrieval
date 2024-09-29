import requests
from sec_downloader import Downloader
from weasyprint import HTML

def get_sec_filing(
        ticker: str, 
        accessionNumber:str,
        pdf_folder_path='./docs',
        edgar_header = 'test@example.com'
        ):
    # Download PDF of EDGAR filing using ticker & accessionNumber

    # Identify filing URL
    dl = Downloader("User-Agent", edgar_header)
    
    metadatas = dl.get_filing_metadatas(f"{ticker}/{accessionNumber}")
    primary_doc_url = metadatas[0].primary_doc_url
    
    # Get HTML string using primary_doc_url
    print(primary_doc_url)
    html = requests.get(primary_doc_url, headers={"User-Agent": edgar_header}).text

    # Remove hyphens from accessionNumber to use as filename 
    filename = accessionNumber.replace("-", "") + ".pdf"
    pdf_savepath = pdf_folder_path + "/" + filename

    # Convert HTML string into PDF and save to pdf_destination_filepath
    htmldoc = HTML(string=html).write_pdf(pdf_savepath)