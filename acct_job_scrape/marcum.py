"""
    JOB SCRAPER for Marcum

    Created:    2020-12-02
    Modified:   2020-12-02
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

By = ws.By  # constants for `find_element` method
CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """A webscraper for Marcum jobs"""

    def __init__(self):
        super().__init__(name='Marcum')

    def extract_page_urls(self, page):
        template = "https://marcum-hr.secure.force.com/recruit/fRecruit__ApplyJob?vacancyNo={}"
        pattern = ws.re.compile(r'vacancyNo=([A-Za-z0-9]*)\"')
        vacancy_nums = ws.re.findall(pattern, page)
        for num in vacancy_nums:
            self.urls_to_scrape.add(template.format(num))

    def standardize_job_data(self, temp):
        """standardize the data and append to scraped data list"""
        job_id = req_id = temp.get('Vacancy No') or ''
        title = temp.get('Vacancy Name') or ''
        department = temp.get('Department Name') or ''
        emp_type = temp.get('Employment Type') or ''
        category = department + ' - ' + emp_type if department else emp_type
        location = temp.get('Work Location') or ''
        url = temp.get('JobUrl') or ''
        record_id = '105-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category,
            location, "", "", "", "", url])

    def extract_card_data(self, card):
        pass

    def extract_page_data(self, url):
        page = self.get_request(url, out_format='soup')
        table = page.find('table', 'detailList')
        rows = table.find_all('tr')
        temp_data = {}
        for row in rows:
            try:
                temp_data[row.label.text.strip()] = row.span.text.strip()
            except AttributeError:
                continue
        temp_data['JobUrl'] = url
        self.standardize_job_data(temp_data)

    def run(self):
        url = "https://marcum-hr.secure.force.com/recruit/fRecruit__ApplyJobList"
        self.create_webdriver(implicit_wait=5, headless=True)
        self.driver.get(url)

        # get all job urls
        while True:
            self.extract_page_urls(self.driver.page_source)
            try:
                self.driver.find_element(By.LINK_TEXT, 'Next').click()
            except ws.NoSuchElementException:
                break
        self.driver.close()

        # extract job data
        for page in self.urls_to_scrape:
            self.extract_page_data(page)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
