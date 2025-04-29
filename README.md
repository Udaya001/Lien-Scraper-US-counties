# Lien Data Scraper for Yuma and Apache Counties

This project is a Python-based web scraping tool designed to extract lien data from the official county websites of **Yuma County** and **Apache County**. The scraper collects structured information and prepares it for further analysis or database insertion.

## Features

- Automated scraping of lien records from Yuma and Apache counties
- Extracts key fields such as:
  - Document Number
  - Recorded Date
  - Grantor / Grantee
  - Document Type
  - Document Image, etc.
- Handles pagination and structured HTML parsing
- Logs activity and errors for monitoring
- Designed for modular expansion to support more counties in the future

## Technologies Used

- Python 3.x
- `requests` / `httpx`
- `BeautifulSoup` (bs4)
- `logging` (for runtime tracking)
