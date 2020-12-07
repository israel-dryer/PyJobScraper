"""
    JOB SCRAPER for BAKER TILLY

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """Webscrape jobs for Baker Tilly"""

    def __init__(self):
        super().__init__(name='BakerTilly')
        
    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        pass

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        for row in page:
            try:
                location = row['location']
            except KeyError:
                location = ''
            try:
                category = row['category']
            except KeyError:
                category = ''
        
            job_id = req_id = row['id']
            title = row['title']
            record_id = '155-' + self.today + str(job_id) + str(req_id)
            url = 'https://www.bakertilly.com/careers/detail/' + row['id']

            self.data_scraped.append([
                record_id, self.today, job_id, req_id, self.name, title, category,
                location, "", "", "", "", url])
    
    def run(self):
        """Run the scraper"""
        url = 'https://www.bakertilly.com/api/icims/search?page={}'
        page_num = 1

        json_data = self.get_request(url.format(page_num), out_format='json')
        total_pages = int(json_data['totalPages'])

        while page_num < total_pages:
            json_data = self.get_request(url.format(page_num), out_format='json')
            job_items = json_data['items']
            self.extract_page_data(job_items)
            page_num += 1

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':

    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
