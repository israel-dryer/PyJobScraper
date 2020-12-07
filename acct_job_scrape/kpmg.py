"""
    JOB SCRAPER for KPMG

    Created:    2020-12-02
    Modified:   2020-12-02
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString
from urllib3.exceptions import InsecureRequestWarning

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """A web scraper for KPMG jobs"""

    def __init__(self):
        super().__init__(name='KPMG')

    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        raise NotImplementedError

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        title = card.a.text
        temp = card.span.text.strip().split('\n')
        location = temp[-1].strip()
        category = temp[0]
        url = ws.re.search(r'https://us-jobs.kpmg.com/careers/JobDetail\?jobId=\d+', card.a['href']).group(0)
        job_id = req_id = ws.re.search(r'\d+', url).group(0)
        record_id = '120-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category,
            location, "", "", "", "", url])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        cards = page.find_all('div', 'resultsData')
        if not cards:
            return False
        for card in cards:
            self.extract_card_data(card)
        return True

    def run(self):
        """Run the scraper"""
        url = 'https://us-jobs.kpmg.com/careers/SearchResults/?jobOffset={}'
        page_num = 0
        self.create_session()
        self.session.verify = False
        ws.requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

        while True:
            page = self.get_request(url.format(page_num), use_session=True, out_format='soup')
            if self.extract_page_data(page):
                page_num += 10
            else:
                break

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
