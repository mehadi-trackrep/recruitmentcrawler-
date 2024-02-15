from loguru import logger as LOGGER
from RecruitmentCrawler.helpers import to_json
from RecruitmentCrawler import helpers as Helpers


class LogPrinter():
    def __init__(self, site_name:str = '', country_name:str = '', response_time: str = "0:00:00.000000", tentative_jobs_count: int = 0, scraped_jobs_count: int = 0):
        self.site_name = site_name
        self.country_name = country_name
        self.response_time = response_time
        self.tentative_jobs_count = tentative_jobs_count
        self.scraped_jobs_count = scraped_jobs_count
        
    def print(self):
        LOGGER.info(to_json({"log_type": "metric", "site_name": self.site_name, "country_name": self.country_name, "response_time": self.response_time, "date": Helpers.get_required_recruiment_date().strftime(
            '%Y-%m-%dT%H:%M:%S'), "tentative_jobs_count": self.tentative_jobs_count, "scraped_jobs_count": self.scraped_jobs_count}))
