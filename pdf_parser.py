import os
import requests
from pdfminer.high_level import extract_text
from turso import TursoDBManager  # Replace 'your_db_file' with the name of your Python file containing TursoDBManager
# Global constant
HOST_URL = "https://entscheidsuche.ch/docs/"


def download_pdf(url, save_path):
    """Download a PDF from a URL to a given path."""
    response = requests.get(url)
    response.raise_for_status()  # Will raise an exception for HTTP errors
    with open(save_path, 'wb') as file:
        file.write(response.content)

def parse_pdf_to_text(pdf_path):
    """Parse the text from a PDF file using PDFMiner."""
    text = extract_text(pdf_path)
    return text

def process_pdfs_and_store():
    db_manager = TursoDBManager()
    rows = db_manager.get_all_rows_e_bern_raw()
    # Print the number of fetched rows
    print(f"Number of rows fetched: {len(rows)}")

    for row in rows:
        file_name, file_path = row[2], row[7]  # Adjust according to your actual database schema
        pdf_url =  HOST_URL + file_path  # Assuming file_path is the URL
        
        # Generate a local save path for the PDF
        pdf_local_path = f"pdf_downloads/{file_name}.pdf"
        os.makedirs(os.path.dirname(pdf_local_path), exist_ok=True)
        
        try:
            # Download, parse, and store
            download_pdf(pdf_url, pdf_local_path)
            pdf_text = parse_pdf_to_text(pdf_local_path)
            db_manager.insert_row_if_not_exists_e_bern_parsed(file_name, file_path, pdf_text)
            os.remove(pdf_local_path)  # Delete the PDF file after processing
            print(f"Processed and removed {file_name}")
        except Exception as e:
            print(f"Failed to process {file_name} due to {e}")
            # Attempt to clean up partially downloaded or processed files
            if os.path.exists(pdf_local_path):
                os.remove(pdf_local_path)

if __name__ == "__main__":

    process_pdfs_and_store()
