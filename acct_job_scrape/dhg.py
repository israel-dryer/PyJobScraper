"""
    JOB SCRAPER for DHG (Dixon Hughes Goodman)

    Created:    2020-11-30
    Modified:   2020-11-30
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'Host': 'jobs.dhg.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/json; charset=utf-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive'
}
URL = ("https://jobs.dhg.com/search-jobs/results?ActiveFacetID=0&CurrentPage=1&RecordsPerPage=88"
       + "&Distance=50&RadiusUnitType=0&Keywords=&Location=&Latitude=&Longitude=&ShowRadius=False"
       + "&CustomFacetName=&FacetTerm=&FacetType=0&SearchResultsModuleName=Search+Results"
       + "&SearchFiltersModuleName=Search+Filters&SortCriteria=0&SortDirection=0&SearchType=5"
       + "&CategoryFacetTerm=&CategoryFacetType=&LocationFacetTerm=&LocationFacetType=&KeywordType="
       + "&LocationType=&LocationPath=&OrganizationIds=&PostalCode=&fc=&fl=&fcf=&afc=&afl=&afcf=")


def parse_json_to_dict(soup):
    """Convert json string to python dictionary"""
    try:
        text = soup.find('script', {'type': 'application/ld+json'}).contents[0]
    except (IndexError, AttributeError):
        return
    try:
        return ws.json.loads(text)
    except ws.json.JSONDecodeError:
        return


class JobScraper(ws.WebScraper):
    """Web scraper for DHG jobs"""

    def __init__(self):
        super().__init__(name='DHG')

    def extract_card_data(self, card):
        pass

    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        json_data = self.get_request(URL, out_format='json', headers=HEADERS)
        raw_html = json_data['results']
        soup = ws.BeautifulSoup(raw_html, 'lxml')
        for tag in soup.find_all('a', 'locationclick'):
            url = 'https://jobs.dhg.com' + tag['href']
            self.urls_to_scrape.add(url)

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        job_id = page['identifier']
        req_id = job_id.split('-')[-1]
        title = page['title']
        city = page['jobLocation']['address']['addressLocality']
        state = page['jobLocation']['address']['addressRegion']
        description = ws.BeautifulSoup(page['description'], 'lxml').text
        url = page['url']
        record_id = '185-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title,
            "", "", city, state, "", description, url])

    def run(self):
        """Run the scraper"""
        self.extract_page_urls(None)

        for url in self.urls_to_scrape:
            soup = self.get_request(url, out_format='soup')
            page = parse_json_to_dict(soup)
            if page:
                self.extract_page_data(page)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
