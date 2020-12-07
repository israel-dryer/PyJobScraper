"""
    JOB SCRAPER for Eisner Amper

    Created:    2020-12-02
    Modified:   2020-12-02
    Author:     Israel Dryer
"""
import wsl.webscraper as ws
from wsl.datatools import DataTools, ConnectionString

CONN_STRING = ConnectionString.build()
INSERT_QUERY = "INSERT INTO jobs.RawJobData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
HEADERS = {
    'Accept': 'application/json,application/xml',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'PLAY_LANG=en-US; PLAY_SESSION=d39c63375db0749b605b32b3e4e8822d31307d60-eisneramper_pSessionId=5sn9og0vbudouaels3ijoi4niu&instance=wd1prvps0001e; wday_vps_cookie=3391531530.64050.0000; TS014c1515=01560d0839b2a918c2a7d99abbddca39a0f5cdc494c92fa54c3201ab6a28e0c59bff2b2aa994f60b91a55e2e57c58b0a9df9efa58a; timezoneOffset=240; cdnDown=0',
    'Host': 'eisneramper.wd1.myworkdayjobs.com',
    'Referer': 'https://eisneramper.wd1.myworkdayjobs.com/EisnerAmper_External',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36 Edg/84.0.522.52',
    'X-Workday-Client': '2020.31.012'
}


def get_next_page(page, page_num):
    """Extract and return url for next page"""
    template = 'https://eisneramper.wd1.myworkdayjobs.com{}/{}?clientRequestID={}'
    uri = page['body']['children'][0]['endPoints'][1]['uri']
    client_request_id = page['sessionSecureToken']
    return template.format(uri, page_num, client_request_id)


class JobScraper(ws.WebScraper):
    """A webscraper for Eisner Amper jobs"""

    def __init__(self):
        super().__init__(name='EisnerAmper')

    def extract_page_urls(self, page):
        pass

    def extract_card_data(self, card):
        """Extract data from a single card; return should reflect final form and return to `scraped_data`"""
        job_id = card['id']
        req_id = card['subtitles'][1]['instances'][0]['text'].split('-')[-1]
        city = card['subtitles'][0]['instances'][0]['text']
        title = card['title']['instances'][0]['text']
        url = 'https://eisneramper.wd1.myworkdayjobs.com' + card['title']['commandLink']
        record_id = '195-' + self.today + str(req_id) + str(job_id)

        self.data_scraped.append([
            record_id, self.today, job_id, req_id, self.name, title, "", "",
            city, "", "", "", url])

    def extract_page_data(self, page):
        """Extract data from page; return should reflect final form and return to `scraped_data`"""
        cards = page['body']['children'][0]['children'][0]['listItems']
        for card in cards:
            self.extract_card_data(card)

    def run(self):
        """Run the scraper"""
        url = "https://eisneramper.wd1.myworkdayjobs.com/EisnerAmper_External?clientRequestID=369870641bd94ccd8b96ead80332674d"
        page_num = 0

        while True:
            try:  # attempt to request json data if exists
                page = self.get_request(url, out_format='json', headers=HEADERS)
            except ws.json.JSONDecodeError:
                break

            self.extract_page_data(page)
            page_num += 50
            url = get_next_page(page, page_num)

        if self.data_scraped:
            DataTools.save_to_database(self.data_scraped, CONN_STRING, INSERT_QUERY)
            print(f"{self.name} >> {len(self.data_scraped)} records")


if __name__ == '__main__':
    print("Starting...")
    scraper = JobScraper()
    scraper.run()
    print("Finished.")
