import requests
import pandas as pd 
from dotenv import load_dotenv
import os
import requests

def refresh_my_filings(
        CIK, 
        filingForm,
        filingDate_from,
        filingDate_to,
):
    """
    Checks EDGAR for new filings and create row in the my_filings.csv
    Returns number of new filings. 
    """

    every_submission = get_every_submission(CIK, filingDate_from, filingDate_to)
    return process_submissions(every_submission, CIK, filingForm, filingDate_from, filingDate_to)

def get_every_submission(
    CIK, 
    filingDate_from,
    filingDate_to,
):
    """
    Fetches every submission using the EDGAR API and returns it as dataframe 
    """

    load_dotenv()
    EDGAR_HEADER = os.getenv('EDGAR_HEADER')

    # 1. Get 1000 most recent submissions as df
    response = requests.get(
        f"https://data.sec.gov/submissions/CIK{CIK}.json", 
        headers={"User-Agent": EDGAR_HEADER}
    )
    if response.status_code != 200: 
        print(response.status_code)
        return
    
    filings = response.json()['filings']
    df = pd.DataFrame.from_dict(filings['recent'])
    
    # 2. Get additional filings and append to df
    additional_filings = filings['files']
    for file in additional_filings:

        filingTo = file['filingTo']
        filingFrom = file['filingFrom']
        filingName = file['name']

        if (filingDate_from <= filingTo) and (filingDate_to >= filingFrom):

            response = requests.get(f"https://data.sec.gov/submissions/{filingName}", 
                            headers={"User-Agent": EDGAR_HEADER})
            if response.status_code != 200: 
                print(response.status_code)
                continue

            temp_df = pd.DataFrame.from_dict(response.json())
            df = pd.concat([df, temp_df])
    
    # 3. Return df
    return df
    
def process_submissions(
        every_submission, 
        CIK, 
        filingForm, 
        filingDate_from,
        filingDate_to
):
    """
    Helper function for update_my_filings. Cleans every_submission, identifies missing submissions then writes to storage. 
    Returns: 
        number of new filings
    """

    my_filings_savepath = os.getenv("MY_FILINGS_SAVEPATH")

    # Data processing
    every_submission = every_submission[
        ( every_submission['form'].isin(filingForm) ) &
        ( every_submission['filingDate'] >= filingDate_from ) & 
        ( every_submission['filingDate'] <= filingDate_to )
    ]
    every_submission.loc[:, "CIK"] = CIK

    # Create file if it's new.
    if not os.path.exists(my_filings_savepath):
        every_submission.to_csv(my_filings_savepath, index=False)
        print(f"Created {my_filings_savepath}")
    
    else:
        my_filings = pd.read_csv(my_filings_savepath)
        every_submission = every_submission[~every_submission['accessionNumber'].isin(my_filings.index)]
        my_filings = pd.concat([my_filings, every_submission])
        
        num_new_filings = len(every_submission)
        my_filings.to_csv(my_filings_savepath, index=False)
        print(f"Updated {my_filings_savepath} with {num_new_filings } new filings")

        return num_new_filings
