"""
    JOB SCRAPER for BDO

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {'Host': 'sjobs.brassring.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
           'Accept': '*/*', 'Accept-Language': 'en-US,en;q=0.5', 'Accept-Encoding': 'gzip, deflate, br',
           'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
           'X-Requested-With': 'XMLHttpRequest',
           'Origin': 'https://sjobs.brassring.com/TgNewUI/Search/Ajax/MatchedJobs',
           'Referer': 'https://sjobs.brassring.com/TGnewUI/Search/Home/Home?partnerid=25776&siteid=5174',
           'Connection': 'keep-alive'
           }
PAYLOAD = {'partnerId': "25776", 'siteId': "5174", 'keyword': "", 'location': "",
           'locationCustomSolrFields': "Location,FORMTEXT1", 'linkId': "", 'Latitude': 0, 'Longitude': 0,
           'pageNumber': '1'}


class JobScraper(ws.WebScraper):
    """A web scraper for BDO jobs"""

    def __init__(self):
        super().__init__(company_name='BDO')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        row_data = {item['QuestionName']: item['Value'] for item in card['Questions']}
        req_id = row_data['reqid']
        job_id = row_data['autoreq']
        title = row_data['jobtitle']
        description = ws.BeautifulSoup(row_data['jobdescription'], 'lxml').text
        url = card['Link']
        city = row_data['formtext5']
        state = row_data['formtext4']
        record_id = '175-' + self.today + str(job_id) + str(req_id)
        return (
            record_id, self.today, job_id, req_id, self.company_name, title, "", "",
            city, state, "", description, url
        )

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        page_data = page['Jobs']['Job']
        if len(page_data) == 0:
            return False
        for card in page_data:
            record = self.extract_card_data(card)
            if record:
                self.data_scraped.append(record)
        return True

    def run(self):
        """Run the scraper"""
        url = 'https://sjobs.brassring.com/TgNewUI/Search/Ajax/ProcessSortAndShowMoreJobs'
        running = True
        page_num = 1

        while running:
            PAYLOAD['pageNumber'] = str(page_num)
            page = self.post_request(url, headers=HEADERS, data=PAYLOAD, format='json')
            if self.extract_page_data(page):
                page_num += 1
                ws.sleep(0.5)
            else:
                break

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.company_name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':

    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
