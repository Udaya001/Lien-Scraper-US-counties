import os
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin,urlparse
import re 
import csv
import shutil 
import concurrent.futures

class LienScraper:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")  # Remove trailing slash
        self.session = requests.Session()
        self.default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        }
        self.output_dir = r"D:\web_scrapper\scrapper-project\output"
        self.text_dir = os.path.join(self.output_dir,"texts")
        self.pdf_dir = os.path.join(self.output_dir,"pdfs")
        self.html_dir = os.path.join(self.output_dir,"htmls")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.text_dir,exist_ok=True)
        os.makedirs(self.pdf_dir,exist_ok=True)
        os.makedirs(self.html_dir,exist_ok=True)

        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def login(self):
        login_url = f"{self.base_url}/recorder/web/loginPOST.jsp"
        payload = {"guest": "true", "submit": "Public Login"}
        
        response = self.session.post(login_url, data=payload, headers=self.default_headers)
        if response.status_code == 200 and response.url != login_url:
            logging.info("Login Successful!")
            return True
        logging.error(f"Login Failed! Status Code: {response.status_code}")
        return False

    def search(self, start_date, end_date):
        search_url = f"{self.base_url}/recorder/eagleweb/docSearchPOST.jsp"
        payload = {
            "RecordingDateIDStart": start_date,
            "RecordingDateIDEnd": end_date,
            "__search_select": ["AFLR", "AL", "ALR", "CL", "FTAX", "FTXR", "HSP", "HSPP", "HSPR", "MECR", "MECH", "MECA", "MECHPR", "RACK", "RACR", "RELL", "REST", "RLR", "STAX", "SUBL"],
        }
        response = self.session.post(search_url, headers=self.default_headers, data=payload)
        if response.status_code == 200:
            logging.info("Search Successful")
            return response.text
        logging.error(f"Search Failed! Status Code: {response.status_code}")
        return None



    def get_search_results(self):
        search_results_url = f"{self.base_url}/recorder/eagleweb/docSearchResults.jsp?searchId=0"
        document_links = set()

        while search_results_url:  # Keep fetching until there are no more pages
            response = self.session.get(search_results_url, headers=self.default_headers)

            if response.status_code != 200:
                logging.error(f"Failed to fetch search results! Status Code: {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "viewDoc.jsp?" in href:
                    absolute_url = urljoin(self.base_url, href)

                    # Fix cases where `/recorder` is missing or `..` appears
                    if "/recorder" not in urlparse(absolute_url).path:
                        absolute_url = self.base_url + "/recorder" + href.lstrip("..")

                    # Remove any unintended `..` in URLs
                    absolute_url = absolute_url.replace("recorder../", "recorder/")

                    document_links.add(absolute_url)

            # Find the "Next" page link
            next_page_element = soup.find("a", string=re.compile(r"\bNext\b", re.IGNORECASE))  # More precise regex
            if next_page_element and next_page_element.get("href"):
                next_page_url = urljoin(self.base_url, next_page_element["href"])
                logging.info(f"Moving to next page: {next_page_url}")
                search_results_url = next_page_url  # **This line updates search_results_url**
            else:
                search_results_url = None  # Stops the loop when there's no Next page


        logging.info(f"Found {len(document_links)} unique document links.")
        return list(document_links)


    def fetch_and_save_html(self, url):
        response = self.session.get(url, headers=self.default_headers)
        if response.status_code != 200:
            logging.error(f"Failed to fetch document page! Status code: {response.status_code}")
            return None

        html_content = response.text
        # # Save the HTML content to a file
        # document_id = url.split("node=")[-1]  # Extract document ID from the URL
        # filename = f"{document_id}.html"
        # filepath = os.path.join(self.html_dir, filename)

        # try:
        #     with open(filepath, "w", encoding="utf-8") as f:
        #         f.write(html_content)  # Save the raw HTML content
        #     logging.info(f"HTML file saved to {filepath}")
        # except Exception as e:
        #     logging.error(f"Error saving HTML file: {e}")
        
        # return filepath
        return html_content


    def extract_data(self, html_content, document_id):#html_file):

        # with open(html_file, "r", encoding="utf-8") as f:
        #     soup = BeautifulSoup(f, 'html.parser')

        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract Document Number
        doc_number = soup.find(text="Document Number").find_next("span", class_="text").get_text(strip=True)
            
        # Extract Recording Date
        recording_date = soup.find(text="Recording Date").find_next("span").get_text(strip=True)
            
        # Extract Number of Pages
        num_pages = soup.find(text="Number Pages").find_next("span", class_="text").get_text(strip=True)
            
         # Extract Total Fees
        total_fees = soup.find(text="Total Fees").find_next("span", class_="text").get_text(strip=True)
            
        # Extract Address Details
        company = soup.find(text="Return Address").find_next("span", class_="text").get_text(strip=True)
        address1 = soup.find(text="Address 1").find_next("span", class_="text").get_text(strip=True)
        address2 = soup.find(text="Address 2").find_next("span", class_="text").get_text(strip=True)
        city = soup.find(text="City").find_next("span", class_="text").get_text(strip=True)
        state = soup.find(text="State").find_next("span", class_="text").get_text(strip=True)
        zip_code = soup.find(text="Zip").find_next("span", class_="text").get_text(strip=True)
            
        # Find the <th> tag containing "Grantor"
        grantor_th = soup.find("th", string=lambda text: text and "Grantor" in text)

        grantor_list = []
        if grantor_th:
            # Find all subsequent <td> tags that contain grantor names
            for td in grantor_th.find_parent("tr").find_next_siblings("tr"):
                grantor_text = td.get_text(strip=True)  # Get clean text
                if grantor_text:
                    grantor_list.append(grantor_text)

            
                # Extract Grantee
        # Find the <th> tag containing "Grantee"
        grantee_th = soup.find("th", string=lambda text: text and "Grantee" in text)

        grantee_list = []
        if grantee_th:
            # Find all subsequent <tr> elements containing <td> with grantee names
            for td in grantee_th.find_parent("tr").find_next_siblings("tr"):
                grantee_text = td.get_text(strip=True)  # Get clean text
                if grantee_text:
                    grantee_list.append(grantee_text)

        
        


        payload = {
            "relatedMode":"allDocs",
            "sourceDocId":document_id
        }
        all_url = f"https://yumacountyaz-recweb.tylerhost.net/recorder/eagleweb/relatedDocsInline.jsp"
        response_docs = self.session.post(all_url, data=payload)
        r_html_content = response_docs.text

        soup1 = BeautifulSoup(r_html_content,'html.parser')

        documents = []
    
        for td in soup1.find_all('td'):
            a_tag = td.find('a', class_='trigger')
            if a_tag:
                doc_name = a_tag.contents[0].strip()
                rdoc_number = a_tag.text.split('\n')[-1].strip()
                doc_link = urljoin(self.base_url,"/recorder/eagleweb/"+a_tag.get('href'))
                documents.append((rdoc_number, doc_name, doc_link))


        # Process each table row
        # for row in soup.find_all('tr', class_='tableRow2'):
        #     link = row.find('a')
        #     if link:
        #         # Extract and append document number
        #         doc_numbers.append(link.get('id', '').strip())
                
        #         # Extract and append document name (text before first <br>)
        #         full_text = link.get_text(separator='|').split('|')
        #         doc_names.append(full_text[0].strip())
                
        #         # Extract and append document link
        #         doc_links.append(link.get('href', '').strip())


        # # related_doc_number = soup.find(text="Document Number").find_next("span", class_="text").get_text(strip=True)
        # related_doc_number_all = soup.find_all(text="Document Number")
        # related_doc_number = related_doc_number_all[1].find_next("span", class_="text").get_text(strip=True)


        # # Extract related document link
        # related_doc_link = None
        # if related_doc_number:
        #     temp_id = related_doc_number.replace("-","0")
        #     doc_id = "DOCC" + temp_id
        #     related_doc_link = f"{self.base_url}/recorder/eagleweb/viewDoc.jsp?node={doc_id}"


        
 
        # Extract PDF download link
        pdf_link = None
        pdf_element = soup.find("a", class_="generator", href=True)
        if pdf_element:
            pdf_link = urljoin(self.base_url, pdf_element["href"])
            
            if "/recorder/eagleweb" not in pdf_link:
                pdf_link = pdf_link.replace(self.base_url, self.base_url + "/recorder/eagleweb")
        logging.info(f"PDF link: {pdf_link}")
            
        return {
                "Document Number": doc_number,
                "Recording Date": recording_date,
                "Number of Pages": num_pages,
                "Total Fees": total_fees,
                "Return Address": {
                    "Company": company,
                    "Address 1": address1,
                    "Address 2": address2,
                    "City": city,
                    "State": state,
                    "Zip": zip_code
                },
                "Grantors": grantor_list,
                "Grantee": grantee_list,
                "Related Documents": documents,
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
                        writer.writerow(["Document ID"] + list(data.keys()))  # Write headers
                    writer.writerow([url.split("node=")[-1]] + list(data.values()))  # Append values

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
            document_id = doc_url.split("node=")[-1] 

            logging.info(f"Processing ({idx}/{len(document_links)}): {doc_url}")

            html_content = self.fetch_and_save_html(doc_url)

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





