import re
import json
import requests
from abc import ABC, abstractmethod
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from msedge.selenium_tools import Edge, EdgeOptions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException


class WebScraper(ABC):
    """A general purpose webscraping class. DO NOT USE this directly; subclass and override the methods"""

    def __init__(self, name):
        self.name = name
        self.today = datetime.today().strftime("%Y-%m-%d")
        self.driver = None
        self.session = None
        self.data_scraped = []
        self.urls_to_scrape = set()

    @abstractmethod
    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        raise NotImplementedError

    @abstractmethod
    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        raise NotImplementedError

    @abstractmethod
    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        raise NotImplementedError

    @abstractmethod
    def run(self):
        """Run the scraper"""
        raise NotImplementedError

    def create_webdriver(self, options=None, headless=False, implicit_wait=None, maximize_window=False, **kwargs):
        """Create a webdriver instead using the Edge web driver"""
        edge_options = EdgeOptions()
        edge_options.use_chromium = True
        edge_options.page_load_strategy = 'eager'
        edge_options.add_argument('log-level=3')
        if headless:
            edge_options.headless = headless
        if options:
            for opt in options:
                edge_options.add_argument(opt)
        self.driver = Edge(options=edge_options, executable_path=r"C:\webdrivers\msedgedriver.exe", **kwargs)
        if implicit_wait:
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

    def post_request(self, url, out_format='soup', use_session=False, **kwargs):
        """Send a post request"""
        if use_session:
            if not self.session:  # a session will be created if not already existing
                self.create_session()
            response = self.session.post(url, **kwargs)
        else:
            response = requests.post(url, **kwargs)
        if out_format == 'soup':
            return BeautifulSoup(response.text, 'lxml')
        elif out_format == 'json':
            return response.json()
        elif out_format == 'text':
            return response.text
        elif out_format == 'response':
            return response
        else:
            raise Exception("Invalid format specificed for 'format'")
