from weasyprint import HTML, CSS
import sys
sys.path.append("/opt/airflow")
from project_config import sec_config

def convert_html_to_pdf(
    html_savepath: str,
    pdf_savepath: str
) -> str:

    with open(html_savepath, 'r', encoding='utf-8') as file:
        html_content = file.read()

    # Define CSS for proper page layout
    css = '''
        @page {
            size: A4;
            margin: 1in;
        }
        
        body {
            margin: 0;
            padding: 0;
            width: 100%;
            max-width: 100%;
        }
        
        /* Ensure all content stays within page bounds */
        div, p, table, section {
            width: 100%;
            max-width: 100%;
            margin-left: 0;
            margin-right: 0;
            box-sizing: border-box;
        }
    '''

    HTML(string=html_content).write_pdf(
        pdf_savepath,
        stylesheets=[CSS(string=css)]
    )

    return pdf_savepath

def convert_submissions_to_pdf(
    submissions_dict: dict
) -> dict:
    """
    Step 3 of DAG. 
    (Submissions is a dictionary with accession number as key.)
    Convert HTML to pdf and append pdf_savepath to submissions
    """
    for accession_number, values in submissions_dict.items():
        
        pdf_savepath = f"{sec_config.SEC_PDF_FOLDERPATH}/{accession_number}.pdf"
        print("values:", values.get("html_savepath"))
        convert_html_to_pdf(values.get("html_savepath"), pdf_savepath)

        values["pdf_savepath"] = pdf_savepath
    
    return submissions_dict