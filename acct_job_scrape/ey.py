"""
    JOB SCRAPER for EY (Ernst & Young)

    Created     :   2020-12-03
    Author      :   Israel Dryer

    2021-08-23  Rebuilt script for new website.
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"

class JobScraper(ws.WebScraper):
    """A web scraper for EY jobs"""

    def __init__(self):
        super().__init__(name='EY')

    def extract_card_data(self, card):
        pass

    def extract_page_urls(self, _):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""

        start_row = 0
        url = 'https://careers.ey.com/ey/search/?q=&location=US&sortColumn=referencedate&sortDirection=desc&startrow={}'

        site = 'https://careers.ey.com'

        while True:
            soup = self.get_request(url.format(start_row), out_format='soup')
            atags = soup.find_all('a', 'jobTitle-link')
            temp = set([site + tag['href'] for tag in atags])
            if (temp - self.urls_to_scrape):
                self.urls_to_scrape.update(temp)
            else:
                break
            start_row += 25

    def extract_page_data(self, url):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        try:
            soup = self.get_request(url, out_format='soup')
            req_id = job_id = url.split('/')[-2]
            record_id = f'100-{self.today}-{job_id}-{req_id}'
            location = soup.find("span", {"data-careersite-propertyid": "city"}).text.strip()
            title = soup.find("span", {"data-careersite-propertyid": "title"}).text.strip()
            description = soup.find("span", {"data-careersite-propertyid": "description"}).text.strip()
        except:
            return

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title,
            "", location, "", "", "", description, url])

    def run(self):
        """Run the scraper"""
        self.extract_page_urls(None)

        for url in self.urls_to_scrape:
            self.extract_page_data(url)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
