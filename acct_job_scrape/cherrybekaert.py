"""
    JOB SCRAPER for Cherry Bekaert

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
PAYLOAD = '''{"opportunitySearch":{"Top":50,"Skip":0,"QueryString":"","OrderBy":[{"Value":"postedDateDesc",
"PropertyName":"PostedDate","Ascending":false}],"Filters":[{"t":"TermsSearchFilterDto","fieldName":4,"extra":null,
"values":[]},{"t":"TermsSearchFilterDto","fieldName":5,"extra":null,"values":[]},{"t":"TermsSearchFilterDto",
"fieldName":6,"extra":null,"values":[]}]},"matchCriteria":{"PreferredJobs":[],"Educations":[],
"LicenseAndCertifications":[],"Skills":[],"hasNoLicenses":false,"SkippedSkills":[]}} '''
HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Length': '495',
    'Content-Type': 'application/json; charset=UTF-8',
    'Host': 'recruiting.ultipro.com',
    'Origin': 'https://recruiting.ultipro.com',
    'Referer': 'https://recruiting.ultipro.com/CHE1006CBH/JobBoard/8effb9c6-91dc-4fae-4091-71d162d6fafe/?q=&o=postedDateDesc',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36 Edg/84.0.522.52'
}


class JobScraper(ws.WebScraper):
    """A webscraper for CBIZ jobs"""

    def __init__(self):
        super().__init__(name='Cherry Bekaert')

    def extract_page_urls(self, page):
        pass

    def extract_page_data(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        job_id = card['Id']
        req_id = card['RequisitionNumber']
        title = card['Title']
        category = card['JobCategoryName']
        city = card['Locations'][0]['Address']['City']
        state = card['Locations'][0]['Address']['State']['Code']
        country = card['Locations'][0]['Address']['Country']['Code']
        url = 'https://recruiting.ultipro.com/CHE1006CBH/JobBoard/8effb9c6-91dc-4fae-4091-71d162d6fafe/OpportunityDetail?opportunityId=' + \
              card['Id']
        record_id = '145-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category, "",
            city, state, country, "", url
        ])

    def run(self):
        """Run the scraper"""
        url = 'https://recruiting.ultipro.com/CHE1006CBH/JobBoard/8effb9c6-91dc-4fae-4091-71d162d6fafe/JobBoardView/LoadSearchResults'
        json_data = self.post_request(url, headers=HEADERS, data=PAYLOAD, out_format='json')
        cards = json_data['opportunities']
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
