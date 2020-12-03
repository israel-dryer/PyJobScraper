"""
    JOB SCRAPER for KPMG

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
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://jobs.pwc.com',
    'Connection': 'keep-alive',
    'Referer': 'https://jobs.pwc.com/ListJobs'
}


class JobScraper(ws.WebScraper):
    """A web scraper for PWC jobs"""

    def __init__(self):
        super().__init__(company_name='PWC')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        job_id = card['JobId']
        req_id = card['DisplayJobId']
        title = card['JobTitle']
        category = card['LongTextField7']
        location = card['LongTextField5'].split('|')[0].strip()
        url = "https://jobs.pwc.com/ShowJob/JobId/" + str(job_id)
        record_id = '135-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.company_name, title, category,
            location, "", "", "", "", url])

    def extract_page_data(self, page):
        for card in page['Data']:
            self.extract_card_data(card)

    def run(self):
        page_num = 1
        template = (
                "https://cmsservice.smashfly.com/api/jobs/v1/jobs/hZtAUIBJAtYt3u6LLr6IZbnp13StNSmiW6NF93TgvUivZ-"
                + "F70L96Q_XDj5YYbxiHlB-6xq-UIZE1?sort=AddedOn-desc&page={}&pageSize=100&group=&filter=&fields=Display"
                + "JobId%2CJobTitle%2CShortTextField4%2CLongTextField7%2CLongTextField5%2CUrlJobTitle")

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
