import os
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin,urlparse,parse_qs
import re 
import csv
import time
import shutil
import concurrent.futures
import email.utils 

class LienScraper:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")  
        self.session = requests.Session()
        
        self.default_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "AjaxRequest": "true",
            "Connection": "keep-alive",
            "Host": "eaglerecorder.co.apache.az.us",
            "Referer": f"{self.base_url}/web/search/DOCSEARCH138S1",
            "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }

    

        self.output_dir = r"D:\web_scrapper\apache\output"
        self.text_dir = os.path.join(self.output_dir,"texts")
        self.pdf_dir = os.path.join(self.output_dir,"pdfs")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.text_dir,exist_ok=True)
        os.makedirs(self.pdf_dir,exist_ok=True)


        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def disclaimer_check(self):
        in_url = f"{self.base_url}/web/user/disclaimer"
        
        response = self.session.post(in_url, headers=self.default_headers)
        
        if response.status_code == 200:
            logging.info("Disclaimer accepted successfully!")

            # Store session cookies
            self.session.cookies.update(response.cookies.get_dict())
            logging.info(f"Session Cookies: {self.session.cookies.get_dict()}")

            return True

        logging.error(f"Failed to accept disclaimer! Status Code: {response.status_code}")
        return False
    

    def extract_jsessionid(self):
        jsessionid = None
        print("Extracting jsessionid from session")
        for cookie in self.session.cookies:
            if cookie.name == 'JSESSIONID':
                jsessionid = cookie.value
                break

        if jsessionid:
            print(f"Found JSESSIONID: {jsessionid}")
            return jsessionid
        else:
            print("JSESSIONID not found in cookies")
            return None
        
    def get_timestamp(self,response):
    # Get the date from response headers
        date_string = response.headers['Date']

        if date_string:
        # Parse the HTTP date format
            tuple_time = email.utils.parsedate_tz(date_string)
        
        # Convert to timestamp (seconds)
            timestamp_seconds = email.utils.mktime_tz(tuple_time)
        
        # Convert to milliseconds
            timestamp_ms = int(timestamp_seconds * 1000)
            return timestamp_ms
        else:
            logging.info("Date header not found")


    def search(self, start_date, end_date,sessionID):
        search_url = f"{self.base_url}/web/searchPost/DOCSEARCH138S1"
        payload = {
            "field_BothNamesID-containsInput": "Contains Any",
            "field_BothNamesID": "",
            "field_GrantorID-containsInput": "Contains Any",
            "field_GrantorID": "",
            "field_GranteeID-containsInput": "Contains Any",
            "field_GranteeID": "",
            "field_RecordingDateID_DOT_StartDate": start_date,
            "field_RecordingDateID_DOT_EndDate": end_date,
            "field_DocumentNumberID": "",
            "field_PlattedLegalID_DOT_Subdivision-containsInput": "Contains Any",
            "field_PlattedLegalID_DOT_Subdivision": "",
            "field_PlattedLegalID_DOT_Lot": "",
            "field_PlattedLegalID_DOT_Block": "",
            "field_PlattedLegalID_DOT_Tract": "",
            "field_PlattedLegalID_DOT_Unit": "",
            "field_PLSSLegalID_DOT_SixteenthSection-containsInput": "Contains Any",
            "field_PLSSLegalID_DOT_SixteenthSection": "",
            "field_PLSSLegalID_DOT_QuarterSection-containsInput": "Contains Any",
            "field_PLSSLegalID_DOT_QuarterSection": "",
            "field_PLSSLegalID_DOT_Section": "",
            "field_PLSSLegalID_DOT_Township": "",
            "field_PLSSLegalID_DOT_Range": "",
            "field_ParcelID": "",
            "field_selfservice_documentTypes-searchInput": ["lien","release"],
            "field_selfservice_documentTypes-containsInput": "Contains Any",
            "field_selfservice_documentTypes": "",
            "field_UseAdvancedSearch": ""
        }

        headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "AjaxRequest": "true",
        "Connection": "keep-alive",
        "Content-Length": "1016",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": f"JSESSIONID={sessionID}; disclaimerAccepted=true",
        "Host": "eaglerecorder.co.apache.az.us",
        "Origin": "https://eaglerecorder.co.apache.az.us",
        "Referer": "https://eaglerecorder.co.apache.az.us/web/search/DOCSEARCH138S1",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

        try:
            logging.info("initiating search post with payload")


            response = self.session.post(search_url, headers=headers, data=payload)
            
            # Check if request was successful
            if response.status_code == 200:
                print(f"Success! Response received. Status code: {response.status_code}")
                logging.info(response.text)
                try:
                        json_data = response.json()              
                        total_pages = json_data.get('totalPages', 1)
                        timestamp = self.get_timestamp(response)
                        result = {
                            "totalPages": total_pages,
                            "timestamp": timestamp
                        }
                        logging.info(total_pages)
                        return result
                except ValueError:
                        logging.error(f"Invalid JSON response:{response}")
                        return None
           
            else:
                logging.error(f"Error: {response.status_code} - {response.reason}")
                return None
        except Exception as e:
            logging.error(f"Exception occurred: {e}")
            return None
    

    def get_search_results(self,total_pages,timestamp):
        page_number = 1
        document_links = []
        for page_number in range(1, total_pages + 1):
            try:
                logging.info(f"Initiating search result for page : {page_number}")
                payload = {
                    'page': page_number,
                    '_': timestamp
                }

                response = self.session.get(f"{self.base_url}/web/searchResults/DOCSEARCH138S1", params=payload, headers=self.default_headers)

                logging.info(f"Result Url: {response.url}")

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    for link in soup.find_all("a", href=True):
                        href = link["href"]
                        if "/web/document/" in href:
                            absolute_url = urljoin(self.base_url, href)
                            logging.info(f"aurl:{absolute_url}")
                            document_links.append(absolute_url)
            except Exception as e:
                logging.error(f"Error occurred: {e}")
                return None
            logging.info("All pages Extracted successfully")

        return document_links
    
    def fetch_html(self, url):
        response = self.session.get(url, headers=self.default_headers)
        if response.status_code != 200:
            logging.error(f"Failed to fetch document page! Status code: {response.status_code}")
            return None

        html_content = response.text

        return html_content
    
    def extract_data(self, html_content,document_id):

        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract Document Number
        doc_number_tag = soup.find("strong", string="Document Number:")
        doc_number = doc_number_tag.find_next("div").text.strip() if doc_number_tag else "N/A"
            
        # Extract Recording Date
        rec_date_tag = soup.find("strong", string="Recording Date:")
        rec_date = rec_date_tag.find_next("div").text.strip() if rec_date_tag else "N/A"

        #Extract Document Type
        doc_type = soup.find("li", class_="ui-li-static ui-body-inherit ui-last-child").text.strip()

        names_section = soup.find('li', {'role': 'heading', 'class': 'ui-li-divider'}, text="Names")
        grantor = []
        grantee = []

        if names_section:
            # Get the next sibling <li> containing the table
            names_content_li = names_section.find_next_sibling('li', class_='ui-li-static')
            if names_content_li:
                # Find all rows in the table within the "Names" section
                table = names_content_li.find('table')
                if table:
                    for row in table.find_all('tr'):
                        # Extract the label (e.g., "Grantor:" or "Grantee:")
                        label_tag = row.find('strong')
                        if label_tag:
                            label = label_tag.get_text(strip=True)
                            # Extract the corresponding name from the next <div>
                            name_container = label_tag.find_next('div')
                            if name_container:
                                # Handle cases where names are in <ul> or plain text
                                if name_container.find('ul'):
                                    names = [item.get_text(strip=True)
                                              for item in name_container.find_all('li')
                                              if not item.find('a') and "Show More..." not in item.get_text(strip=True)
                                            ]
                                else:
                                    names = [name_container.get_text(strip=True)]
                                
                                if label == "Grantor:":
                                    grantor.extend(names)
                                elif label == "Grantee:":
                                    grantee.extend(names)

        # Locate the "Notes" section by finding the <li> element with role="heading" and text "Notes"
        notes_heading = soup.find('li', {'role': 'heading', 'class': 'ui-li-divider'}, text='Notes')
        if notes_heading:
            # Get the next sibling <li> which contains the actual notes content
            notes_content_li = notes_heading.find_next_sibling('li', class_='ui-li-static')
            if notes_content_li:
                # Extract the text content of the notes
                notes = notes_content_li.get_text(strip=True)
                
            else:
                notes = ["N/A"]  # Fallback if structure changes
        else:
            notes = ["N/A"]  # Fallback if no Notes section

        # Extract Legal Information
        legal_section = soup.find('li', {'role': 'heading', 'class': 'ui-li-divider'}, text="Legal")
        if legal_section:
            legal_content_li = legal_section.find_next_sibling('li', class_='ui-li-static')
            legal_info = legal_content_li.get_text(strip=True) if legal_content_li else "N/A"
        else:
            legal_info = "N/A"


        related_url = f"{self.base_url}/web/document/relatedDocuments/{document_id}"
        response_rdocs = self.session.get(related_url,headers=self.default_headers)
        rhtml_content = response_rdocs.text

        soup1 = BeautifulSoup(rhtml_content,'html.parser')
        # Extract document details
        related_docs = []

        # Find all collapsible rows
        collapsible_rows = soup1.find_all('div', class_='ss-row related-table-row')
        for row in collapsible_rows:
            # Extract document type
            rdoc_type = row.find('td', class_='related-doc-type')
            rdoc_type = rdoc_type.text.strip() if rdoc_type else "N/A"

            # Extract recording date
            related_rdate = row.find('td', class_='related-doc-recording-date')
            related_rdate = related_rdate.text.strip() if related_rdate else "N/A"

            # Extract external ID (with error handling)
            external_id_link = row.find('a', class_='document-link')
            external_id = external_id_link.text.strip() if external_id_link else "N/A"

            # Extract associated names
            names_div = row.find('div', style="width:50%; float:left")
            names = [name.strip() for name in names_div.stripped_strings] if names_div else []

            # Append extracted details to the list
            related_docs.append({
                'Document Type': rdoc_type,
                'Recording Date': related_rdate,
                'External ID': external_id,
                'Associated Names': names
            })
            

        logging.info(document_id)
        pdf_link = f"{self.base_url}/web/document/servepdf/DEGRADED-{document_id}.1.pdf/{doc_number}.pdf?index"

            
        return {
                "Document ID": document_id,
                "Document Type":doc_type,
                "Document Number": doc_number,
                "Recording Date": rec_date,
                "Grantors": grantor,
                "Grantees": grantee,
                "Legal": legal_info,
                "Notes": notes,
                "Related Documents": related_docs,
                "PDF Link":pdf_link
            }
    

    def download_pdf(self, pdf_url):
        # Downloads and saves the PDF file efficiently using a larger buffer.
        if not pdf_url:
            logging.warning("No PDF link found, skipping download.")
            return
        
        filename = os.path.basename(urlparse(pdf_url).path)
        filepath = os.path.join(self.pdf_dir, filename)

        try:
            response = self.session.get(pdf_url, headers=self.default_headers, stream=True)
            if response.status_code == 200:
                with open(filepath, "wb") as pdf_file, response as r:
                    shutil.copyfileobj(r.raw, pdf_file)
                logging.info(f"Downloaded PDF: {filename}")
            else:
                logging.error(f"Failed to download PDF! Status Code: {response.status_code}")
        except requests.RequestException as e:
            logging.error(f"Error downloading PDF: {e}")

    def download_pdfs_concurrently(self, pdf_urls):
        #   `Downloads multiple PDFs concurrently.
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:  # 5 threads
            executor.map(self.download_pdf, pdf_urls)


    
    def save_text(self, url, data):
        #Saves extracted text from multiple documents into a single CSV file
        filename = "all_documents.csv"  # Unified CSV file
        filepath = os.path.join(self.text_dir, filename)

        write_headers = not os.path.exists(filepath)  # Check if file exists

        try:
            with open(filepath, "a", newline="", encoding="utf-8") as f:  # Open in append mode
                writer = csv.writer(f)
                
                if isinstance(data, dict):
                    if write_headers:
                        writer.writerow( list(data.keys()))  # Write headers
                    writer.writerow(list(data.values()))  # Append values

                elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    if write_headers:
                        writer.writerow(["Document ID"] + list(data[0].keys()))  # Write headers
                    for item in data:
                        writer.writerow([url.split("node=")[-1]] + list(item.values()))  # Append each row

            logging.info(f"Data appended to {filepath}")

        except Exception as e:
            logging.error(f"Error saving file: {e}")

    def process_documents(self, document_links):
        #Processes each document page: extracts text & downloads PDFs
        for idx, doc_url in enumerate(document_links, 1):
            parsed_url = urlparse(doc_url)
            document_id = parsed_url.path.split("/")[-1] 
            logging.info(f"Processing ({idx}/{len(document_links)}): {doc_url}")

            html_content = self.fetch_html(doc_url)

            if html_content:
                data = self.extract_data(html_content,document_id)
                self.save_text(doc_url, data)

                # Ensure PDF links are in list format
                pdf_urls = data.get("PDF Link")
                if not isinstance(pdf_urls, list):
                    pdf_urls = [pdf_urls] if pdf_urls else []  # Convert None to empty list

                self.download_pdfs_concurrently(pdf_urls)

                logging.info(f"Processed document: {doc_url}")
            else:
                logging.error(f"Failed to process document: {doc_url}")
        

        logging.info("Processing complete!")
        









   
