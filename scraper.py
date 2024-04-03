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
            
            if link.endswith('.json'):
                data = {'file_name': file_name}  # Initialize the data dict with file_name
                json_data = fetch_and_parse_json(full_link)
                data.update(json_data)  # Update the data dict with JSON data
                #print(f"Extracted JSON data for {full_link}: {json_data}")
                db_manager.insert_or_update_row_with_data(data)
                    
if __name__ == "__main__":
    host_url = "https://entscheidsuche.ch"
    target_url = "/docs/BE_ZivilStraf/"
    scrape_and_store(host_url, target_url)
