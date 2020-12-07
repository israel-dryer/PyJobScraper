"""
    JOB SCRAPER for EY (Ernst & Young)

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'eygbl.referrals.selectminds.com',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36 Edg/86.0.622.63'
}


class JobScraper(ws.WebScraper):
    """A web scraper for EY jobs"""

    def __init__(self):
        super().__init__(name='EY')

    def extract_card_data(self, card):
        pass

    def extract_page_urls(self, _):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        url = "https://eygbl.referrals.selectminds.com/jobs/search/99762557"
        next_page = "https://eygbl.referrals.selectminds.com/jobs/search/99762557/page{}"
        page_num = 1
        last_count = 0

        soup = self.get_request(url, headers=HEADERS, out_format='soup')
        while True:
            for tag in soup.find_all('a', 'job_link'):
                self.urls_to_scrape.add(tag['href'])
            if len(self.urls_to_scrape) == last_count:
                break
            last_count = len(self.urls_to_scrape)
            page_num += 1
            soup = self.get_request(next_page.format(page_num), headers=HEADERS)

    def extract_page_data(self, url):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        soup = self.get_request(url, headers=HEADERS, out_format='soup')
        job_id = soup.find('input', {'name': 'Job.id'})['value']
        req_id = soup.find('dd', 'job_external_id').span.text
        title = soup.find("h1", "title").text.strip()
        category = soup.find('dl', 'field_category').dd.span.text
        location = soup.find('h4', 'primary_location').text.replace('🔍', '').strip()
        description = soup.find('div', 'job_description').text.strip()
        record_id = '100-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title,
            category, location, "", "", "", description, url])

    def run(self):
        """Run the scraper"""
        self.extract_page_urls(None)

        for url in self.urls_to_scrape:
            self.extract_page_data(url)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
