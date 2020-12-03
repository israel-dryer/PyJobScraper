"""
    JOB SCRAPER for RSM

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'Host': 'cmsservice.smashfly.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://jobs.rsmus.com',
    'Connection': 'keep-alive',
    'Referer': 'https://jobs.rsmus.com/ListJobs'
}


class JobScraper(ws.WebScraper):
    """A web scraper for RSM jobs"""

    def __init__(self):
        super().__init__(company_name='RSM')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        job_id = card['JobId']
        req_id = card['DisplayJobId'].replace('req', '').split('_')[0]
        title = card['JobTitle']
        category = card['ShortTextField9']
        location = card['Location']
        url = "https://jobs.rsmus.com/ShowJob/JobId/" + str(job_id)
        record_id = '150-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.company_name, title, category,
            location, "", "", "", "", url])

    def extract_page_data(self, page):
        for card in page['Data']:
            self.extract_card_data(card)

    def run(self):
        page_num = 1
        template = (
                "https://cmsservice.smashfly.com/api/jobs/v1/jobs/hZtAUIBJAtYt3u6LLr6IZTmj9c2V39Q6ouvFD2DCuMtzWQbmN-"
                + "GSIsuEta_wfak70?sort=AddedOn-asc&page={}&pageSize=100&group=&filter=&fields=JobTitle%2CShortTextField9"
                + "%2CLocation%2CDisplayJobId%2CUrlJobTitle")

        while True:
            page = self.get_request(template.format(page_num), headers=HEADERS, out_format='json')
            if not page['Data']:
                break
            self.extract_page_data(page)
            page_num += 1

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.company_name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
