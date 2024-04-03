import requests
from bs4 import BeautifulSoup
import json
from turso import TursoDBManager

def fetch_and_parse_json(url):
    """Fetches a JSON file and extracts relevant data."""
    response = requests.get(url)
    data = response.json()
    
    extracted_data = {
        'datum': data['Datum'],
        'forderung': next((item['Text'] for item in data['Abstract'] if 'de' in item['Sprachen']), ''),
        'signatur': data.get('Signatur', ''),
        'source': data.get('Spider', ''),
        'file_path': data.get('PDF', {}).get('Datei', ''),
        'pdf_url': data.get('PDF', {}).get('URL', ''),
        'checksum': data.get('PDF', {}).get('Checksum', ''),
        'case_number': " ".join(data.get('Num', [])),
        'scrapy_job': data.get('ScrapyJob', ''),
        'fetch_time_utc': data.get('Zeit UTC', '')
    }
    
    return extracted_data


def download_pdf(url):
    """Downloads a PDF and returns its content."""
    response = requests.get(url)
    return response.content


def scrape_and_store(host_url, target_url):
    response = requests.get(host_url + target_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    db_manager = TursoDBManager()

    for tr in soup.select("#table-content tr")[1:]:  # Skip the header row
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue  # Skip rows that don't have enough data

        links = [a['href'] for a in cells[0].find_all('a', href=True)]
        for link in links:
            full_link = host_url + link
            print(f"Processing {full_link}...")
            # Extract the file name here to ensure it's always filled
            file_name = link.split('/')[-1].rsplit('.', 1)[0]
            #print(f"File name: {file_name}")
            data = {'file_name': file_name}  # Initialize the data dict with file_name
            
            if link.endswith('.json'):
                json_data = fetch_and_parse_json(full_link)
                data.update(json_data)  # Update the data dict with JSON data
                # No need for pdf_blob here since it's a JSON file
                #print(f"Extracted JSON data for {full_link}: {json_data}")

                db_manager.insert_or_update_row_with_data(data)
            elif link.endswith('.pdf'):

                pdf_blob = download_pdf(full_link)
                print(f"Downloaded PDF for {full_link}")
                
                # Since it's a PDF, the relevant JSON data fields should be set to None or their defaults
                # Only update the dict with PDF blob if it's a PDF file
                #data['pdf_blob'] = pdf_blob
                db_manager.insert_or_update_row_with_data(data, pdf_blob=pdf_blob)
                    
if __name__ == "__main__":
    host_url = "https://entscheidsuche.ch"
    target_url = "/docs/BE_Verwaltungsgericht/"
    scrape_and_store(host_url, target_url)
