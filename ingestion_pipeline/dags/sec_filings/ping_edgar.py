import requests
import pandas as pd 
import os
import requests
from qdrant_client import QdrantClient, models

import sys
sys.path.append("/opt/airflow")
from project_config import qdrant_config, sec_config

class submission():
    
    def __init__(self, cik, accession_number, primary_document):
        self.cik = cik
        self.accession_number = accession_number
        self.primary_document = primary_document
    
    def filing_url(self):
        """
        EDGAR urls look like this: https://www.sec.gov/Archives/edgar/data/1650696/000095017022026539/lsf-20221012.htm
        """
        accession_number_no_hyphen = self.accession_number.replace("-", "")
        return f"{sec_config.EDGAR_FILING_BASE_URL}/{self.cik}/{accession_number_no_hyphen}/{self.primary_document}"   

def get_submissions_df(
    cik: str, 
    filingDate_from:str,
    filingDate_to:str,
    filingForm:str
) -> pd.DataFrame:
    """
    Fetches all submission between the specified timeframe, and returns accession_numbers as list. 
    Because the EDGAR API sucks, the only way to get historical records is to start with the most recent filings and work your way back
    """
    edgar_api_headers = {"User-Agent": sec_config.EDGAR_EMAIL}
    # 1. Get 1000 most recent submissions as df
    response = requests.get(
        f"https://data.sec.gov/submissions/CIK{cik}.json", 
        headers=edgar_api_headers
    )

    if response.status_code != 200: 
        print(response.status_code)
        return
    
    filings = response.json()['filings']
    df = pd.DataFrame.from_dict(filings['recent'])
    
    # 2. Paginate and append to df
    additional_filings = filings['files']
    for file in additional_filings:

        filingTo = file['filingTo']
        filingFrom = file['filingFrom']
        filingName = file['name']

        if (filingDate_from <= filingTo) and (filingDate_to >= filingFrom):
            response = requests.get(
                f"https://data.sec.gov/submissions/{filingName}", 
                headers=edgar_api_headers
                )
            if response.status_code != 200: 
                print(response.status_code)
                continue

            temp_df = pd.DataFrame.from_dict(response.json())
            df = pd.concat([df, temp_df])
    
    # 3. Filter
    df = df[df['form'].isin(filingForm)]
    df = df[(df["filingDate"] >= filingDate_from) & (df['filingDate'] <= filingDate_to)]

    return df
    
def missing_submissions(
    cik: str,
    filingForm: list[str] = sec_config.filingForm,
    filingDate_from: str = sec_config.filingDate_from,
    filingDate_to:str = sec_config.filingDate_to,
    qdrant_url:str = qdrant_config.QDRANT_URL,
    qdrant_port:str = qdrant_config.QDRANT_PORT,
    qdrant_collection_name:str = qdrant_config.QDRANT_COLLECTION_NAME
) -> list[submission]:
    """
    Return missing filings for one company. 
    Each submissions is a dict with cik, accession_number and primary_document
    """
    # submissions is list of accession_numbers for all submissions in the timeframe
    submissions_df = get_submissions_df(cik, filingDate_from , filingDate_to, filingForm)

    qdrant_client = QdrantClient(qdrant_url, port=qdrant_port)
    missing_submissions_for_cik = [] # Populate this as we go 

    for index, row in submissions_df.iterrows():
        # Search my db for current accession_number
        accession_number = row["accessionNumber"]
        primary_document = row['primaryDocument']

        results = qdrant_client.scroll(
            collection_name=qdrant_collection_name,
            scroll_filter=models.Filter(
                must=[models.FieldCondition(
                    key="accession_number",
                    match=models.MatchValue(value=accession_number),
                )]
            )
        )
        # If there were no records in my db...
        if not results[0]:
            missing_submissions_for_cik.append(submission(cik, accession_number, primary_document))
    
    return missing_submissions_for_cik

def ping_edgar()-> dict:
    """
    Step 1 of DAG. 
    Ping SEC website and return submissions missing from my db. Submissions is a dictionary with acccession value as key.
    """
    missing_submissions_dict = {}
    for ticker, cik in sec_config.watchlist.items():
        
        for s in missing_submissions(cik):

            missing_submissions_dict.update({
                s.accession_number: {
                    "cik": s.cik,
                    "filing_url": s.filing_url()
                    }
                })
    
    print("typetype:", type(missing_submissions_dict.get("0000950170-22-003138")))
    return missing_submissions_dict