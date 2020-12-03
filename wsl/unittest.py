import csv
import pyodbc
from datetime import datetime
from wsl.datatools import DataTools, ConnectionString
from wsl.webscraper import WebScraper


class UnitTest:
    """A set of test for testing module functionality"""

    @staticmethod
    def save_to_csv():
        """Save test data to a csv file"""
        today = datetime.now().strftime('%Y%m%d%H%M%S')
        headers = ['FirstName', 'LastName']
        data = [('Israel', 'Dryer'), ('Judy', 'Dryer'), ('Abigail', 'Dryer')]
        filenamepath = r'c:\temp\save_to_csv_test_' + today + '.csv'
        DataTools.save_to_csv(data, filenamepath, headers)

        # test the resulting file
        try:
            with open(filenamepath, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                data = list(reader)
        except FileNotFoundError as e:
            print("FAILED >> Save to CSV File >> No file found. The file did not save")
            return

        try:
            assert data[0][0] == 'FirstName'
            assert data[1][1] == 'Dryer'
        except AssertionError:
            print("FAILED >> Save to CSV File >> The test data does not match the data on file")
            return

        print("SUCCESS >> Save to CSV File")

    @staticmethod
    def get_request_text():
        """Send a get request and return a soup object"""
        url = "http://drudgereport.com/"
        scraper = WebScraper("Drudge")
        textdata = scraper.get_request(url, out_format='text')
        try:
            assert textdata.lower().startswith("<html>")
        except AssertionError:
            print("FAILED >> Get Request Text >> Did not find <HTML> in response text.")
            return
        print("SUCCESS >> Get Request Text")

    @staticmethod
    def get_request_soup():
        """Send a get request and return a soup object"""
        url = "http://drudgereport.com/"
        scraper = WebScraper("Drudge")
        soup = scraper.get_request(url)
        try:
            assert 'drudge' in soup.title.text.lower()
        except AssertionError:
            print("FAILED >> Get Request Soup >> Did not find 'Drudge' in response text.")
            return
        print("SUCCESS >> Get Request Soup")

    @staticmethod
    def get_request_json():
        """Send a get request and return a json object"""
        url = "https://api.github.com/users/mralexgray/repos"
        scraper = WebScraper("JsonData")
        result = scraper.get_request(url, out_format='json')
        try:
            assert type(result[0]) == dict
        except AssertionError:
            print("FAILED >> Get Request JSON >> Did not return a `dict` type object")
            return
        print("SUCCESS >> Get Request JSON")

    @staticmethod
    def create_webdriver():
        """Create a webdriver and navigate to a webpage"""
        url = "http://drudgereport.com/"
        scraper = WebScraper("Drudge")
        scraper.create_webdriver(headless=True)
        scraper.driver.get(url)
        try:
            assert 'drudge' in scraper.driver.title.lower()
        except AssertionError:
            print("FAIL >> Create and run Webdriver >> did not find 'drudge' in webdriver get request")
        scraper.driver.close()
        print("SUCCESS >> Create and run Webdriver")

    @staticmethod
    def build_connection_string():
        """Create a connection string"""
        expected = 'DRIVER={ODBC DRIVER 17 FOR SQL SERVER}; SERVER=GTLPF1MZF5M\IZZY_SQL_001; DATABASE=MERCURY; TRUSTED_CONNECTION=YES;'
        actual = ConnectionString.build()
        try:
            assert actual == expected
        except AssertionError:
            print(
                f"FAIL >> Build connection string >> resulting string was not as expected:\nExcpected->{expected}\nActual->{actual}")
        print("SUCCESS >> Build connection string")

    @staticmethod
    def run_all():
        """Run all unit tests"""
        print("-" * 50 + "\nBeginning unit tests\n" + "-" * 50)
        UnitTest.save_to_csv()
        UnitTest.get_request_text()
        UnitTest.get_request_soup()
        UnitTest.get_request_json()
        UnitTest.create_webdriver()
        UnitTest.build_connection_string()
        print("-" * 50 + "\nCompleted unit tests\n" + "-" * 50)


if __name__ == '__main__':
    UnitTest.run_all()
