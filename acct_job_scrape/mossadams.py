"""
    JOB SCRAPER for Moss Adams

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
PAYLOAD = """{"multilineEnabled":true,"sortingSelection":{"sortBySelectionParam":"3","ascendingSortingOrder":"false"},"fieldData":{"fields":{"KEYWORD":"","CATEGORY":"","LOCATION":""},"valid":true},"filterSelectionParam":{"searchFilterSelections":[{"id":"POSTING_DATE","selectedValues":[]},{"id":"LOCATION","selectedValues":[]},{"id":"JOB_FIELD","selectedValues":[]}]},"advancedSearchFiltersSelectionParam":{"searchFilterSelections":[{"id":"ORGANIZATION","selectedValues":[]},{"id":"LOCATION","selectedValues":[]},{"id":"JOB_FIELD","selectedValues":[]},{"id":"JOB_NUMBER","selectedValues":[]},{"id":"URGENT_JOB","selectedValues":[]},{"id":"EMPLOYEE_STATUS","selectedValues":[]},{"id":"STUDY_LEVEL","selectedValues":[]},{"id":"WILL_TRAVEL","selectedValues":[]},{"id":"JOB_SHIFT","selectedValues":[]}]},"""
HEADERS = {'Accept': 'application/json, text/javascript, */*; q=0.01',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'en-US,en;q=0.9',
           'Connection': 'keep-alive',
           'Content-Length': '798',
           'Content-Type': 'application/json',
           'Host': 'mossadams.taleo.net',
           'Origin': 'https://mossadams.taleo.net',
           'Referer': 'https://mossadams.taleo.net/careersection/6/jobsearch.ftl?lang=en&amp;portal=4160751617',
           'Sec-Fetch-Dest': 'empty',
           'Sec-Fetch-Mode': 'cors',
           'Sec-Fetch-Site': 'same-origin',
           'tz': 'GMT-05:00',
           'tzname': 'America/New_York',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36 Edg/86.0.622.63'}


class JobScraper(ws.WebScraper):
    """A web scraper for MossAdams jobs"""

    def __init__(self):
        super().__init__(company_name='MossAdams')

    def extract_page_urls(self, _):
        """Extract job urls to extract full job details"""
        url = "https://mossadams.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=4160751617"
        page_num = 1

        while True:
            payload = PAYLOAD + '"pageNo":' + str(page_num) + "}"
            json_data = self.post_request(url, format='json', headers=HEADERS, data=payload)
            if len(json_data['requisitionList']) == 0:
                break

            for job in json_data['requisitionList']:
                job_url = "https://mossadams.taleo.net/careersection/6/jobdetail.ftl?job=" + job['contestNo']
                self.urls_to_scrape.add(job_url)

            page_num += 1

    def extract_card_data(self, card):
        pass

    def extract_page_data(self, url):
        soup = self.get_request(url, out_format='soup')
        raw_text = soup.find('input', id='initialHistory').get('value')

        # clean up ascii characters through replacement
        subs = [('%22', '"'), ('%3C', '<'), ('%3E', '>'), ('%24', '$'), ('%5C', '"'),
                ('%0A', ''), ('%26', '&'), ('!', ''), ('%27', "'"), ('&nbsp', ' '), ('!', '')]

        for old, new in subs:
            raw_text = raw_text.replace(old, new)

        # extract text from cleaned up `job_data`
        post_text = ws.BeautifulSoup(raw_text, 'lxml').text

        # extract job details
        job_id = req_id = ws.re.search(r'Job\sNumber\":\s(\d+)', post_text).group(1)
        title = ws.re.search(r'position\":\s(.*?)\s-\s\(', post_text).group(1)
        location = ws.re.search(r'\|([A-Za-z\s]*?,\s\w+)', post_text).group(1)
        record_id = '115-' + self.today + str(job_id) + str(job_id)

        # job description
        temp = ws.re.search(r'\|\|\|(...+)\|\|\|false', post_text).group(1)
        if '|||' in temp:
            description = temp[temp.find('|||') + 3:]
        else:
            description = temp

        # consolidate the data record
        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.company_name, title, "",
            location, "", "", "", description, url])

    def run(self):
        # extract all job urls
        self.extract_page_urls(None)

        # extract all job data
        for url in self.urls_to_scrape:
            self.extract_page_data(url)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.company_name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
