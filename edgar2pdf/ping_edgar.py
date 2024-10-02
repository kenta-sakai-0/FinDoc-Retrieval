from refresh_my_filings import refresh_my_filings
from get_missing_pdf import get_missing_pdf
from refresh_configs import watchlist, filingForm, filingDate_from, filingDate_to

num_new_filings = 0
for ticker, CIK in watchlist.items():
    num_new_filings += refresh_my_filings(CIK, filingForm, filingDate_from, filingDate_to)

if num_new_filings > 0:
    get_missing_pdf()