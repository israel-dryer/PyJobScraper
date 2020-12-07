"""
    JOB SCRAPER for Deloitte

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

By = ws.By
CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """A webscraper for Deloitte jobs"""

    def __init__(self):
        super().__init__(name='Deloitte')

    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        raise NotImplementedError

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        job_id = req_id = card.find_element(By.TAG_NAME, 'a').get_attribute('data-ph-at-job-id-text')
        title = card.find_element(By.TAG_NAME, 'a').get_attribute('data-ph-at-job-title-text')
        category = card.find_element(By.TAG_NAME, 'a').get_attribute('data-ph-at-job-category-text')
        location = card.find_element(By.TAG_NAME, 'a').get_attribute('data-ph-at-job-location-text')
        description = card.find_element(By.CLASS_NAME, 'job-description').text
        record_id = '160-' + self.today + str(job_id) + str(req_id)
        url = 'https://jobs2.deloitte.com/us/en/job/' + str(req_id) + '/'

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category,
            location, "", "", "", description, url])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        cards = self.driver.find_elements(By.CLASS_NAME, 'jobs-list-item')
        if not cards:
            return False
        for card in cards:
            self.extract_card_data(card)
        return True

    def run(self):
        """Run the scraper"""
        self.create_webdriver(headless=True)
        next_page = "https://jobs2.deloitte.com/us/en/Experienced-all-jobs"

        while True:
            self.driver.get(next_page)
            result = self.extract_page_data(None)
            if not result:
                break
            next_page = self.driver.find_element(By.XPATH, '//a[@aria-label="Next"]').get_attribute('href')
            if not next_page:
                break

        self.driver.close()

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
