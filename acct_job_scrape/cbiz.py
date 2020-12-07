"""
    JOB SCRAPER for CBIZ

    Created:    2020-12-01
    Modified:   2020-12-01
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"


class JobScraper(ws.WebScraper):
    """A webscraper to extract jobs for CBIZ"""

    def __init__(self):
        super().__init__(name='CBIZ')

    def extract_card_data(self, card):
        pass

    def extract_page_urls(self, page):
        """Extract urls from the page for further scraping; return to `urls_to_scrape`"""
        table_rows = page.find_all('tr', 'job-result')
        if table_rows:
            for row in table_rows:
                job_url = 'https://careers.cbiz.com/' + row.a['href']
                self.urls_to_scrape.add(job_url)
            return True
        else:
            return False

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        page.encoding = 'utf-8'
        soup = ws.BeautifulSoup(page.text, 'lxml')
        raw_text = soup.find('script', {'type': 'application/ld+json'}).decode()
        clean_text = raw_text.replace('<script type="application/ld+json">', '')
        clean_text = clean_text.replace('</script>', '')
        json_data = ws.json.loads(clean_text)
        try:
            job_id = req_id = json_data['identifier']['value']
        except KeyError:
            return

        title = json_data['title']
        try:
            city = json_data['jobLocation']['address']['addressLocality']
        except KeyError:
            city = ''
        try:
            state = json_data['jobLocation']['address']['addressRegion']
        except KeyError:
            state = ''
        try:
            category = json_data['occupationalCategory'][1]
        except KeyError:
            category = ''
        try:
            description = ws.BeautifulSoup(json_data['description'], 'lxml').text
            description = description.replace('\xa0', '').replace('\u200b', '').strip()
        except KeyError:
            description = ''
        record_id = '180-' + self.today + str(job_id) + str(req_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, category, "",
            city, state, "", description, page.url
        ])

    def run(self):
        """Run the scraper"""
        url = 'https://careers.cbiz.com/en-US/search?pagenumber={}'
        page_num = 1

        while True:
            page = self.get_request(url.format(page_num))
            result = self.extract_page_urls(page)
            page_num += 1
            if not result:
                break

        for url in self.urls_to_scrape:
            page = self.get_request(url, 'response')
            self.extract_page_data(page)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
