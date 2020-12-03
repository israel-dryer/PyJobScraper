import re
import json
import requests
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from msedge.selenium_tools import Edge, EdgeOptions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException


class WebScraper:
    """A general purpose webscraping class. DO NOT USE this directly; subclass and override the methods"""

    def __init__(self, company_name):
        self.company_name = company_name
        self.today = datetime.today().strftime("%Y-%m-%d")
        self.driver = None
        self.session = None
        self.data_scraped = []
        self.urls_to_scrape = set()

    def create_webdriver(self, options=None, headless=False, implicit_wait=3, maximize_window=False, **kwargs):
        """Create a webdriver instead using the Edge web driver"""
        edge_options = EdgeOptions()
        edge_options.use_chromium = True
        edge_options.page_load_strategy = 'eager'
        edge_options.add_argument('log-level=3')
        if headless:
            edge_options.add_argument('disable-gpu')
            edge_options.add_argument('headless')
        if options:
            for opt in options:
                edge_options.add_argument(opt)
        self.driver = Edge(options=edge_options, **kwargs)
        self.driver.implicitly_wait(implicit_wait)
        if maximize_window:
            self.driver.maximize_window()

    def create_session(self):
        """Create a persistent session"""
        self.session = requests.Session()

    def get_request(self, url, out_format='soup', use_session=False, **kwargs):
        """Send a get request; Format options: soup, json, text, response"""
        if use_session:
            if not self.session:  # a session will be created if not already existing
                self.create_session()
            response = self.session.get(url, **kwargs)
        else:
            response = requests.get(url, **kwargs)
        if out_format == 'soup':
            return BeautifulSoup(response.text, 'lxml')
        elif out_format == 'json':
            return response.json()
        elif out_format == 'text':
            return response.text
        elif out_format == 'response':
            return response
        else:
            raise Exception("Invalid format specified for 'format'")

    def post_request(self, url, format='soup', use_session=False, **kwargs):
        """Send a post request"""
        if use_session:
            if not self.session:  # a session will be created if not already existing
                self.create_session()
            response = self.session.post(url, **kwargs)
        else:
            response = requests.post(url, **kwargs)
        if format == 'soup':
            return BeautifulSoup(response.text, 'lxml')
        elif format == 'json':
            return response.json()
        elif format == 'text':
            return response.text
        elif format == 'response':
            return response
        else:
            raise Exception("Invalid format specificed for 'format'")

    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        raise NotImplementedError

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        raise NotImplementedError

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        raise NotImplementedError

    def run(self):
        """Run the scraper"""
        raise NotImplementedError
