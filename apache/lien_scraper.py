import os
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin,urlparse
import re 
import csv
import time
import email.utils 

class LienScraper:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")  # Remove trailing slash
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

    

        self.output_dir = r"D:\web_scrapper\scrapper-project\output"
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

        #JSESSIONID saved in a variable
        if jsessionid:
            print(f"Found JSESSIONID: {jsessionid}")
            # with open('session_id.txt', 'w') as f:
            #     f.write(jsessionid)
            # print("JSESSIONID saved to session_id.txt")
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
            "field_selfservice_documentTypes-searchInput": "lien",
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
        for page_number in range(1, total_pages + 1):
            try:
                logging.info(f"Initiating search result for page : {page_number}")
                payload = {
                    'page': page_number,
                    '_': timestamp
                }
                document_links = []

                response = self.session.get(f"{self.base_url}/web/searchResults/DOCSEARCH138S1", params=payload, headers=self.default_headers)

                logging.info(f"Result Url: {response.url}")

                if response.status_code == 200:
                    logging.info("OK")
                    soup = BeautifulSoup(response.text, "html.parser")
                    logging.info(response)
                    for link in soup.find_all("a", href=True):
                        href = link["href"]
                        if "/web/document/" in href:
                            absolute_url = urljoin(self.base_url, href)
                            document_links.append(absolute_url)
            except Exception as e:
                logging.error(f"Error occurred: {e}")
                return None
            logging.info("All pages Extracted successfully")

        return document_links
        









   