import requests
import sys
sys.path.append("/opt/airflow")
from project_config import sec_config

def download_filing_as_html(
    filing_url,
    html_savepath
)-> str:

    html = requests.get(filing_url, headers={"User-Agent": sec_config.EDGAR_EMAIL}).text
    with open(html_savepath, 'w') as f:
        f.write(html)

def download_submissions_as_html(
    submissions_dict: dict
) -> dict:
    """
    Step 2 of DAG. 
    (Submissions is a dictionary with accession number as key.)
    Download filing as HTML, and append html_savepath to submissions
    """
    for accession_number, values in submissions_dict.items():
        
        html_savepath = f"{sec_config.SEC_HTML_FOLDERPATH}/{accession_number}.html"
        download_filing_as_html(values.get("filing_url"), html_savepath)

        values["html_savepath"] = html_savepath
    
    return submissions_dict