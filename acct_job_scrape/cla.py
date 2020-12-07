"""
    JOB SCRAPER for CLA (Clifton Larson Allen)

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.webscraper import json
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'Host': 'cliftonlarsonallen.wd1.myworkdayjobs.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Accept': 'application/json,application/xml',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://cliftonlarsonallen.wd1.myworkdayjobs.com/CLA'
}


class JobScraper(ws.WebScraper):
    """A webscraper for Clifton Larson Allen jobs"""

    def __init__(self):
        super().__init__(name='CliftonLarsonAllen')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        job_id = req_id = card['subtitles'][0]['instances'][0]['text']
        title = card['title']['instances'][0]['text']
        location = card['subtitles'][1]['instances'][0]['text']
        url = 'https://cliftonlarsonallen.wd1.myworkdayjobs.com' + card['title']['commandLink']
        record_id = '165-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, "",
            location, "", "", "", "", url
        ])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        cards = page['body']['children'][0]['children'][0]['listItems']
        if not cards:
            return False
        for card in cards:
            self.extract_card_data(card)

    def run(self):
        """Run the scraper"""
        url = "https://cliftonlarsonallen.wd1.myworkdayjobs.com/CLA"
        template_url = "https://cliftonlarsonallen.wd1.myworkdayjobs.com{}/{}?clientRequestID={}"
        page_num = 0

        while True:
            try:
                page = self.get_request(url, out_format='json', headers=HEADERS)
            except json.JSONDecodeError:
                break
            self.extract_page_data(page)
            page_num += 50
            uri = page['body']['children'][0]['endPoints'][1]['uri']
            client_request_id = page['sessionSecureToken']
            url = template_url.format(uri, page_num, client_request_id)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
