"""
    A collection of webscraper for extracting open job postings for
    top accounting firms.

    Created:    2020-12-03
    Modified:   2020-12-03
    Author:     Israel Dryer

    #TODO Figure out a way to handle timeout errors on Marcum
    #TODO DHG does not appear to be pickin up all the jobs available... got 86 / site says 126
"""
from acct_job_scrape import *
from threading import Thread
from time import perf_counter

bots = [
    armanino.JobScraper(),
    bakertilly.JobScraper(),
    bdo.JobScraper(),
    bkd.JobScraper(),
    cbiz.JobScraper(),
    cherrybekaert.JobScraper(),
    cla.JobScraper(),
    cohnreznick.JobScraper(),
    crowe.JobScraper(),
    deloitte.JobScraper(),
    dhg.JobScraper(),
    eidebailly.JobScraper(),
    eisneramper.JobScraper(),
    ey.JobScraper(),
    kpmg.JobScraper(),
    marcum.JobScraper(),
    mossadams.JobScraper(),
    plantemoran.JobScraper(),
    pwc.JobScraper(),
    rsm.JobScraper()
]


def run():
    """Run all scrapers in the acct scraper library"""
    threadlist = [Thread(target=bot.run) for bot in bots]

    for thread in threadlist:
        thread.start()

    for thread in threadlist:
        thread.join()


if __name__ == '__main__':
    time_started = perf_counter()
    print('Starting...')
    run()
    #e = ey.JobScraper()
    #e.run()
    time_elapsed = perf_counter() - time_started
    print(f'Finished in {time_elapsed:,.0f} seconds')
