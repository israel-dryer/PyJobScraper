"""
    JOB SCRAPER for BKD

    Created:    2020-11-30
    Modified:   2020-05-17
    Author:     Israel Dryer

    2021-05-17
        Website changed; job description is no longer available on the card; but will require navigating to
        each individual page.
"""
import re
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """description of class"""

    def __init__(self):
        super().__init__(name='BKD')

    def extract_page_urls(self, page):
        pass

    def extract_page_data(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""

        try:
            title = card.a.text.strip()
            url = card.a.get('href')
            spans = card.find(
                'div', 'article__header__text__subtitle').find_all('span')

            job_id = req_id = re.search(r'\d+', spans[1].text).group(0)
            location = spans[0].text.strip()
            record_id = f'125-{self.today}{job_id}{req_id}'

            return (record_id, self.today, job_id, req_id, self.name, title, "", location,
                    "", "", "", "", url)
        except AttributeError:
            return

    def run(self):
        """Run the scraper"""
        next_page = "https://bkd.avature.net/experiencedcareers/SearchJobs/?jobRecordsPerPage=6&jobOffset={}"
        page_offset = 0

        while True:
            soup = self.get_request(next_page.format(page_offset))
            cards = soup.find_all('article', 'article article--result')

            for card in cards:
                record = self.extract_card_data(card)
                if record:
                    self.data_scraped.append(record)

            if len(cards) == 1:
                break
            page_offset += 6

        if self.data_scraped:
            DataTools.save_to_database(
                self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
