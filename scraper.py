import requests
from bs4 import BeautifulSoup
import threading
from db_bern import MySQLDBManager  # Adjust this import according to your project structure
from pdf_parser import PDFScraperAndStorer  # Adjust this import as well
import os

def fetch_and_parse_json(url):
    response = requests.get(url)
    data = response.json()
    extracted_data = {
        'datum': data['Datum'],
        'forderung': next((item['Text'] for item in data.get('Abstract', []) if 'de' in item.get('Sprachen', [])), ''),
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

def scrape_and_store(host_url, target_url, thread_name):
    try:
        response = requests.get(host_url + target_url)
        soup = BeautifulSoup(response.content, 'html.parser')
    
        db_manager = MySQLDBManager()
        pdf_processor = PDFScraperAndStorer()
    
        for tr in soup.select("#table-content tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue
    
            links = [a['href'] for a in cells[0].find_all('a', href=True)]
            for link in links:
                full_link = host_url + link
                #print(f"{thread_name} processing {full_link}...")
                file_name = link.split('/')[-1].rsplit('.', 1)[0]
                
                if link.endswith('.json'):
                    data = {'file_name': file_name}

                    #-------------------
                    # Remove leading and trailing slashes and split
                    parts = target_url.strip("/").split("/")
                    # The last element is the directory name
                    directory_name = parts[-1]
                    nas_parsed_text_path = f"/mnt/z/entscheidsuche/{directory_name}/parsed/{file_name}.txt"
                    if not os.path.exists(nas_parsed_text_path):
                    #-------------------
                        
                        json_data = fetch_and_parse_json(full_link)
                        data.update(json_data)
                        db_manager.insert_or_update_row_with_data(data)
                        pdf_processor.process_one_pdf_and_store(data['file_name'], data['file_path'])

                    #-------------------
                    #else :
                    #    print(f"{thread_name} already processed {full_link}.") 
                    #-------------------
                        
    except Exception as e:
        print(f"{thread_name} encountered an error: {e}. Restarting...")
        threading.Thread(target=scrape_and_store, args=(host_url, target_url, thread_name)).start()

if __name__ == "__main__":
    host_url = "https://entscheidsuche.ch"
    target_urls = [
        "/docs/BE_Anwaltsaufsicht/",
        "/docs/BE_BVD/",
        "/docs/BE_Steuerrekurs/",
        "/docs/BE_Verwaltungsgericht/",
        "/docs/BE_Weitere/",
        "/docs/BE_ZivilStraf/"
    ]

    for index, target_url in enumerate(target_urls):
        thread_name = f"Thread-{target_url}"
        thread = threading.Thread(target=scrape_and_store, args=(host_url, target_url, thread_name), name=thread_name)
        thread.start()

    main_thread = threading.current_thread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        t.join()

    print("Scraping and storage completed.")
