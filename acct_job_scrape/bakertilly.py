"""
    JOB SCRAPER for BAKER TILLY
    Created:    2020-12-01
    Modified:   2020-01-19
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.common import exceptions
from selenium.webdriver.common.by import By

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
        page_num = 1
        wait = WebDriverWait(self.driver, 10)
        
        while True:
            item_is_clickable = expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'span[itemprop="title"]'))
            url = "https://careers.bakertilly.com/jobs?page={}&sortBy=relevance&tags1=Experienced&limit=100"
            self.driver.get(url.format(page_num))
            try:
                wait.until(item_is_clickable)
            except exceptions.TimeoutException:
                pass

            cards = self.driver.find_elements_by_class_name('mat-expansion-panel')
            if not cards:
                break
            for card in cards:
                job_title = card.find_element_by_css_selector('[itemprop="title"]').text
                req_id = job_id = card.find_element_by_css_selector('.req-id span').text
                job_location = card.find_element_by_css_selector('.location').text.replace('\n', ', ')
                job_category = card.find_element_by_css_selector('.categories').text
                job_url = 'https://careers.bakertilly.com/jobs/' + req_id
                record_id = '155-' + self.today + str(req_id) + str(job_id)

                self.data_scraped.append((
                    record_id, self.today, job_id, req_id, self.name, job_title, job_category,
                    job_location, "", "", "", "", job_url
                ))

            page_num += 1

    def run(self):
        """Run the scraper"""
        url = 'https://careers.bakertilly.com/jobs/'
        self.create_webdriver(headless=False)  # doesn't seem to work with headless
        self.driver.get(url)

        self.extract_page_data(None)

        self.driver.quit()

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
