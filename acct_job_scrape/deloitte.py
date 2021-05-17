"""
    JOB SCRAPER for Deloitte

    Created:    2020-12-03
    Modified:   2021-04-20
    Author:     Israel Dryer

    2021-04-20 - complete rebuild as site changed.
"""
import wsl.webscraper as ws
from lxml import html
from wsl.datatools import DataTools, ConnectionString
import re

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """A webscraper for Deloitte jobs"""

    def __init__(self):
        super().__init__(name='Deloitte')

    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        offset = 0

        while True:
            soup = self.get_request(f'https://apply.deloitte.com/careers/SearchJobsAJAX?s=1&jobOffset={offset}')
            links = soup.find_all('a')
            if not links:
                break
            for link in links:
                self.urls_to_scrape.add(link.get('href'))
            offset += 20


    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        job_id = req_id = re.search(r'Requisition code: (\d+)', card.text).group(1)
        title = card.find('h2', 'article__header__text__title').text.strip()
        category = card.find('div', 'article__header__text__subtitle').text.replace('\n', '').replace('   ', '').strip()
        description = card.find('article', 'article article--details').text.replace('\n', '').replace('   ', '').strip()
        record_id = '160-' + self.today + str(job_id) + str(req_id)
        url = 'https://apply.deloitte.com/careers/JobDetail/' + str(req_id) + '/'

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category,
            "", "", "", "", description, url])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        pass

    def run(self):
        """Run the scraper"""
        self.extract_page_urls(None)
        if not len(self.urls_to_scrape):
            print(f"{self.name} >> {len(self.data_scraped)} records")
            return

        for url in list(self.urls_to_scrape):
            try:
                print(url)
                card = self.get_request(url)
                self.extract_card_data(card)
            except:
                continue

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
