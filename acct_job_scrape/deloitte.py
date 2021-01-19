"""
    JOB SCRAPER for Deloitte

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common import exceptions
from selenium.webdriver.common.by import By

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
        data_tag = card.find_element_by_tag_name('a')
        job_id = req_id = data_tag.get_attribute('data-ph-at-job-id-text')
        title = data_tag.get_attribute('data-ph-at-job-title-text')
        category = data_tag.get_attribute('data-ph-at-job-category-text')
        location = data_tag.get_attribute('data-ph-at-job-location-text')
        description = card.find_element_by_class_name('job-description').text
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
        wait = WebDriverWait(self.driver, 10)
        condition = expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'h4[data-ph-at-id="searchresults-job-title"]'))

        while True:
            self.driver.get(next_page)
            try:
                wait.until(condition)
            except exceptions.TimeoutException:
                break

            result = self.extract_page_data(None)
            if not result:
                break
            next_page = self.driver.find_element_by_css_selector('a[aria-label="Next"]').get_attribute('href')
            if not next_page:
                break

        self.driver.quit()

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
