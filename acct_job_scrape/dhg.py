"""
    JOB SCRAPER for DHG (Dixon Hughes Goodman)

    Created:    2020-11-30
    Modified:   2021-03-22
    Author:     Israel Dryer

    2021-03-22 > Site changed; adjusted get request and json parsing logic.
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
URL = ("https://jobs.dhg.com/?method=cappservicesPortal.getPortalWidgetListData&_dc=1616424559756" +
       "&listId=opportunitylist-5&columnList=id,positiontitletext,url,location&localKeywords=" +
       "&categoryID=2&portalModelID=1&page=1&start=1&limit=500")


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
        json_data = self.get_request(URL, out_format='json')['query']['data']
        for record in json_data:
            self.urls_to_scrape.add(record['url'])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        job_id = page['identifier']
        req_id = job_id.split('-')[-1]
        title = page['title']

        try:
            location = page['jobLocation'][0] if isinstance(page['jobLocation'], list) else page['jobLocation']
            city = location['address']['addressLocality']
            state = location['address']['addressRegion']
        except:
            city = state = ''

        description = ws.BeautifulSoup(page['description'], 'lxml').text
        record_id = '185-' + self.today + str(job_id) + str(req_id)

        return record_id, self.today, job_id, req_id, self.name, title, "", "", city, state, "", description

    def run(self):
        """Run the scraper"""
        self.extract_page_urls(None)

        for url in self.urls_to_scrape:
            soup = self.get_request(url, out_format='soup')
            page = parse_json_to_dict(soup)
            if page:
                record = self.extract_page_data(page)
                self.data_scraped.append(record + (url,))

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
