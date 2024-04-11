import os
import requests
from pdfminer.high_level import extract_text
from db_bern import MySQLDBManager
import shutil
import re

class PDFScraperAndStorer:
    HOST_URL = "https://entscheidsuche.ch/docs/"

    def __init__(self):
        self.db_manager = MySQLDBManager()

    @staticmethod
    def clean_text(text):
        text_with_spaces = re.sub(r'\r\n|\r|\n', ' ', text)
        cleaned_text = re.sub(r'[^\x20-\x7E\n\u00C0-\u00FF]+', ' ', text_with_spaces)
        cleaned_text = re.sub(r'[{}]+', '', cleaned_text)
        return cleaned_text

    @staticmethod
    def download_pdf(url, save_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            file.write(response.content)

    @staticmethod
    def parse_pdf_to_text(pdf_path):
        text = extract_text(pdf_path)
        return PDFScraperAndStorer.clean_text(text)

    def process_pdfs_and_store(self):
        rows = self.db_manager.get_all_rows_e_bern_raw()
        print(f"Number of rows fetched: {len(rows)}")

        for row in rows:
            file_name, file_path = row[2], row[7]
            self.process_one_pdf_and_store(file_name, file_path)

    def process_one_pdf_and_store(self, file_name, file_path):
        base_name = os.path.basename(file_path)
        directory_name = os.path.dirname(file_path)

        nas_pdf_path = f"/mnt/z/entscheidsuche/{directory_name}/PDF/{base_name}"
        nas_parsed_text_path = f"/mnt/z/entscheidsuche/{directory_name}/parsed/{base_name.replace('.pdf', '.txt')}"
        pdf_url = self.HOST_URL + file_path

        if not os.path.exists(nas_pdf_path):
            pdf_local_path = f"pdf_downloads/{base_name}"
            os.makedirs(os.path.dirname(pdf_local_path), exist_ok=True)
            try:
                self.download_pdf(pdf_url, pdf_local_path)
                os.makedirs(os.path.dirname(nas_pdf_path), exist_ok=True)
                shutil.copyfile(pdf_local_path, nas_pdf_path)
                os.remove(pdf_local_path)
                print(f"PDF copied to NAS: {base_name}")
            except Exception as e:
                print(f"Failed to download or copy PDF {base_name} due to {e}")
                return

        if not os.path.exists(nas_parsed_text_path):
            try:
                pdf_text = self.parse_pdf_to_text(nas_pdf_path)
                os.makedirs(os.path.dirname(nas_parsed_text_path), exist_ok=True)
                with open(nas_parsed_text_path, "w") as f:
                    f.write(pdf_text)
                print(f"Text parsed and stored: {base_name}")
                self.db_manager.insert_row_if_not_exists_e_bern_parsed(file_name, file_path, pdf_text)
            except Exception as e:
                print(f"Failed to parse or store text {base_name} due to {e}")
        else:
            with open(nas_parsed_text_path, "r") as f:
                pdf_text = f.read()
                try:
                    self.db_manager.insert_row_if_not_exists_e_bern_parsed(file_name, file_path, pdf_text)
                    print(f"Text verified and stored in database: {base_name}")
                except Exception as e:
                    print(f"Failed to verify or store text in database {base_name} due to {e}")

if __name__ == "__main__":
    scraper_and_storer = PDFScraperAndStorer()
    scraper_and_storer.process_pdfs_and_store()
