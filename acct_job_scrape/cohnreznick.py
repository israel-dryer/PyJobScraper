"""
    JOB SCRAPER for Cohn Reznick

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


def parse_json_data(raw_html):
    """Parse json text and return dictionary"""
    pattern = ws.re.compile(r'JSON\.parse\(\'(...+);')
    result = ws.re.search(pattern, raw_html).groups()[0][:-2]
    json_data = ws.json.loads(result)
    return json_data


class JobScraper(ws.WebScraper):
    """A webscraper to extract jobs for Cohn Reznick"""

    def __init__(self):
        super().__init__(name='CohnReznick')

    def extract_page_urls(self, page):
        pass

    def extract_page_data(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        job_id = req_id = card['Id']
        title = card['Title']
        location = card['Location']
        category = card['Department']
        url = card['Url']
        record_id = '190-' + self.today + str(job_id) + str(req_id)
        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category,
            location, "", "", "", "", url])

    def run(self):
        """Run the scraper"""
        url = "https://www.cohnreznick.com/careers/career-opportunities"
        raw_html = self.get_request(url, out_format='text')
        cards = parse_json_data(raw_html)
        for card in cards:
            self.extract_card_data(card)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
