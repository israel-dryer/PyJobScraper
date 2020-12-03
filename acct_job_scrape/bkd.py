"""
    JOB SCRAPER for BKD

    Created:    2020-11-30
    Modified:   2020-11-30
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Accept-Encoding': 'gzip, deflate, br'
}


class JobScraper(ws.WebScraper):
    """description of class"""

    def __init__(self):
        super().__init__(company_name='BKD')

    def extract_page_urls(self, page):
        pass

    def extract_page_data(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        location = card.find('span', text='Job Locations').find_next().text.strip()

        job_id = req_id = card.find('dt', text="Job ID").find_next().text.strip()
        if job_id.count('-') > 0:
            req_id = job_id.split('-')[1]

        title = card.find('span', text="Job Title").find_next().text.strip()
        description = card.find('div', 'description').get_text()
        description = ws.re.sub(r'[\r\n\xa0]', ' ', description).strip()
        url = card.a.get('href')
        record_id = f'125-{self.today}{job_id}{req_id}'

        return (record_id, self.today, job_id, req_id, self.company_name, title, "", location,
                "", "", "", description, url)

    def run(self):
        """Run the scraper"""
        next_page = "https://careers-bkd.icims.com/jobs/search?in_iframe=1"

        while True:
            soup = self.get_request(next_page, use_session=True, headers=HEADERS)
            cards = soup.find_all('div', 'row')

            for card in cards[1:]:
                record = self.extract_card_data(card)
                if record:
                    self.data_scraped.append(record)

            next_page_tag = soup.find('span', text="Next page of results").find_previous()
            if 'invisible' in next_page_tag.get('class'):
                break

            next_page = next_page_tag.get('href')
            if not next_page:
                break

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.company_name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
