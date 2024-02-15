import datetime
from loguru import logger as LOGGER
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from RecruitmentCrawler.spiders import ArbetsformedlingenSVSpiderV2, IndeedSVSpider, IndeedFISpider, OikotieFISpider


class SpiderRunner:
    def __init__(self, process):
        self.process = process

    def run_spiders(self, all_spiders=False):
        spiders = self.get_spiders(all_spiders=all_spiders)
        for spider_name in spiders:
            LOGGER.info("Running spider {}".format(spider_name))
            self.process.crawl(spider_name)
        self.process.start()

    def get_spiders(self, all_spiders=False):
        if all_spiders:
            return list(self.process.spiders.list())

        spiders = [
            ArbetsformedlingenSVSpiderV2, 
            IndeedSVSpider, IndeedFISpider,
            OikotieFISpider
        ]

        return spiders


def main():
    setting = get_project_settings()
    process = CrawlerProcess(setting)
    runner = SpiderRunner(process=process)

    start_time = datetime.datetime.now()
    LOGGER.info("Project settings:")
    for key, val in setting.items():
        LOGGER.info("{}:{}".format(key, val))
    LOGGER.info("#" * 90)

    # run listed spider
    runner.run_spiders(all_spiders=False)
    LOGGER.info("Crawling is completed! Execution time: {}".format(datetime.datetime.now() - start_time))

if __name__ == "__main__":
    main()