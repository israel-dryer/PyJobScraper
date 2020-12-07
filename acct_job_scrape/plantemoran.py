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


class JobScraper(ws.WebScraper):
    """A web scraper for Plante Moran jobs"""

    def __init__(self):
        super().__init__(name='Plante Moran')

    def extract_card_data(self, card):
        pass

    def extract_page_urls(self, _):
        url = "https://plantemoran.avature.net/careers/"
        while True:
            soup = self.get_request(url, out_format='soup')
            for tag in soup.find_all('h3', 'listSingleColumnItemTitle'):
                self.urls_to_scrape.add(tag.a['href'])
            next_page = soup.find('a', 'paginationNextLink')
            if not next_page:
                break
            url = next_page['href']

    def extract_page_data(self, url):
        soup = self.get_request(url)
        title = soup.find('meta', {'property': 'og:title'})['content']
        description = soup.find('meta', {'property': 'og:description'})['content'].replace('&nbsp', ' ')
        location = soup.find('span', 'MultipleDataSetFieldValue').text
        job_id = req_id = url.split('/')[-1]
        record_id = '200-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, "",
            location, "", "", "", description, url])

    def run(self):
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
