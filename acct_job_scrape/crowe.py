"""
    JOB SCRAPER for Crowe

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
URL = ("https://cmsservice.smashfly.com/api/jobs/v1/jobs/"
       + "hZtAUIBJAtYt3u6LLr6IZU4feIW6ILdzG_UALBXus_BeaNzYB65W1iIVmowQ95LSRINELCq7sKw1?"
       + "sort=AddedOn-desc&page={}&pageSize=500&group=&filter=&fields="
       + "DisplayJobId%2CJobTitle%2CShortTextField1%2CMediumTextField1%2CCity%2CState%2CUrlJobTitle")
HEADERS = {
    'Host': 'cmsservice.smashfly.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://jobs.crowe.com',
    'Connection': 'keep-alive',
    'Referer': 'https://jobs.crowe.com/ListJobs'
}


class JobScraper(ws.WebScraper):
    """A webscraper for Crowe jobs"""

    def __init__(self):
        super().__init__(company_name='Crowe')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        req_id = card['DisplayJobId'].split('_')[0]
        job_id = card['JobId']
        title = card['JobTitle']
        city = card['City']
        state = card['State']
        category = card['ShortTextField1']
        record_id = '130-' + self.today + str(job_id) + str(req_id)
        url = 'https://jobs.crowe.com/ShowJob/JobId/' + str(job_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.company_name, title,
            category, "", city, state, "", "", url])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        for card in page:
            self.extract_card_data(card)

    def run(self):
        """Run the scraper"""
        page_num = 1
        while True:
            page = self.get_request(URL.format(page_num), out_format='json', headers=HEADERS)
            if not page['Data']:
                break
            self.extract_page_data(page['Data'])
            page_num += 1

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.company_name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
