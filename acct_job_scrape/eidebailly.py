"""
    JOB SCRAPER for Eide Bailly

    Created:    2020-12-02
    Modified:   2020-12-02
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'content-length': '779',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://recruiting.adp.com',
    'referer': 'https://recruiting.adp.com/srccar/public/RTI.home?c=1151551&d=ExternalCareerSite',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36 Edg/84.0.522.52'
}


def set_payload(page):
    """Set payload by page number"""
    param1 = '''{"filters":[{"name":"allLocations","label":"Where you will work"},{"name":"grp","label":"What you love to do"},{"name":"zzreqPosType","label":"How much you will work"}],"results":{"pageTitle":"Search Results","zeroResultsMessage":"We're sorry but we have no job openings at this time that match your search criteria. Please try another search.","searchFailureMessage":"Oops! Something went wrong.  Search has encountered a problem. Try searching again","resultsFoundLabel":"results found","bookmarkText":"Bookmark This","pageSize":"100","sortOrder":"00001000","shareText":"Share","fields":[{"name":"ptitle","label":"Published Job Title"},{"name":"allLocations","label":"All Locations"},{"name":"zzreqPosType","label":"Full Time/ Part Time"}]},"pagefilter":{"page":'''
    param2 = str(page)
    param3 = '},"rl":"enUS"}'
    return param1 + param2 + param3


class JobScraper(ws.WebScraper):
    """A web scraper for Eide Bailly jobs"""

    def __init__(self):
        super().__init__(company_name='Eide Bailly')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        """Override : extract data for each individual job and save to scraped data"""
        job_id = card['id']
        req_id = card['num']
        title = card['ptitle']
        city = card['city']
        state = card['state']
        description = card['description']
        url = card['url']
        record_id = '140-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.company_name, title, "", "",
            city, state, "", description, url])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        for card in page:
            self.extract_card_data(card)

    def create_session_with_cookies(self):
        """Get cookies for inserting into standard get request"""
        self.create_webdriver(headless=True)
        cookies = self.driver.get_cookies()
        self.driver.close()
        self.create_session()
        self.session.get('https://recruiting.adp.com/srccar/public/RTI.home?c=1151551&d=ExternalCareerSite')
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'])

    def get_page_pagetotal(self, page_num, url):
        """Get and return the page data and total pages"""
        payload = set_payload(page_num)
        json_data = self.post_request(url, format='json', use_session=True, headers=HEADERS, data=payload)
        page_total = int(json_data['pages'])
        page_data = json_data['jobs']
        return page_data, page_total

    def run(self):
        """Run the scraper"""
        url = "https://recruiting.adp.com/srccar/public/rest/1/1215551/search/"
        page_num = 1
        self.create_session_with_cookies()

        # find total pages to scrape
        _, page_total = self.get_page_pagetotal(page_num, url)

        # scrape all pages
        while page_num < page_total:
            page, _ = self.get_page_pagetotal(page_num, url)
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
