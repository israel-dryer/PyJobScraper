"""
    JOB SCRAPER for ARMANINO

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer
"""
from random import random
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """webscraper for Armanino jobs"""

    def __init__(self):
        super().__init__(name='Armanino')

    def extract_card_data(self, card):
        pass

    def extract_page_urls(self, _):
        url = "https://careers-armaninollp.icims.com/jobs/search?ss=1&hashed=-435620445&in_iframe=1"
        last_count = 0
        while True:
            soup = self.get_request(url, out_format='soup')
            for tag in soup.find_all('a', 'iCIMS_Anchor'):
                self.urls_to_scrape.add(tag['href'])
            url = soup.find('div', 'iCIMS_PagingBatch').find_next_sibling()['href']
            if len(self.urls_to_scrape) == last_count:
                break
            last_count = len(self.urls_to_scrape)

    def extract_page_data(self, url):
        soup = self.get_request(url, out_format='soup', use_session=True)
        try:
            json_data = ws.json.loads(soup.find('script', {'type': 'application/ld+json'}).contents[0])
        except AttributeError:
            return  # The job url has expired

        job_id = req_id = soup.find("span", text="Job ID").find_next_sibling().text.strip()
        title = json_data['title']
        city = json_data['jobLocation'][0]['address']['addressLocality']
        state = json_data['jobLocation'][0]['address']['addressRegion']
        country = json_data['jobLocation'][0]['address']['addressCountry']
        category = json_data['occupationalCategory']
        description = ws.BeautifulSoup(json_data['description'], 'lxml').text.replace('\xa0', ' ')
        record_id = '170-' + '2020-12-03' + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title,
            category, "", city, state, country, description, url])

    def run(self):
        self.create_session()
        self.extract_page_urls(None)

        for url in self.urls_to_scrape:
            self.extract_page_data(url)
            ws.sleep(random())

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
