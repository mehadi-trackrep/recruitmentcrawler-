# -*- coding: utf-8 -*-
import justext

from operator import itemgetter
from bs4 import BeautifulSoup

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.spiders import CrawlSpider, Rule

from ..helpers import Json, fix_time_format, string_to_md5
from ..items import RecruitmentCrawlerItem
from RecruitmentCrawler.utils import LogPrinter


class FinnNOSpider(CrawlSpider):
    name = 'finn_no'
    allowed_domains = ['finn.no']
    start_urls = ['https://www.finn.no/job/fulltime/search.html?page={}&sort=1'.format(i) for i in range(1, 5)]

    custom_settings = {"DEPTH_LIMIT": 24}  # TODO: Check here

    rules = (
        Rule(LinkExtractor(
            allow=r".+job.+ad.html?.*",
            unique=True
        ), callback='parse_item', follow=False),
    )

    def __init__(self, tentative_jobs_count: int = 0, scraped_jobs_count: int = 0, name: str = None, **kwargs):
        super(FinnNOSpider, self).__init__(name, **kwargs)
        self.tentative_jobs_count = tentative_jobs_count
        self.scraped_jobs_count = scraped_jobs_count    

    def parse_item(self, response):
        url = response.url
        self.logger.info("Depth: {} ProcessingURL: {}".format(response.meta['depth'], url))

        next_pages = []
        for rule in FinnNOSpider.rules:
            extractor = rule.link_extractor.extract_links(response)
            for link in extractor:
                next_pages.append(link.url)

        self.logger.debug("NextPages: {}".format("\n".join(next_pages)))

        self.tentative_jobs_count += 1
        if self.is_leaf_node(url):
            
            yield self.populate_items(response=response)
            self.scraped_jobs_count += 1
        else:
            for nexturl in next_pages:
                yield scrapy.Request(url=nexturl, callback=self.parse_item)

    def populate_items(self, response):
        self.logger.debug("ParseItems: {}".format(response.url))

        item_loader = ItemLoader(item=RecruitmentCrawlerItem(), response=response)

        soup_obj = BeautifulSoup(response.text, 'html.parser')

        title = soup_obj.find('title').string
        content = self.parse_content(response)
        abstract = self.get_abstract(content=content)
        create_time = self.get_created_time(soup_obj)
        extra_info = self.get_job_info(response)

        item_loader.add_value('title', title)
        item_loader.add_value('text', content)
        item_loader.add_value('summary', abstract)
        item_loader.add_value('timestamp', create_time)
        item_loader.add_value("extra_info", extra_info)
        # item_loader.add_value('triggers', 'recruitment')
        item_loader.add_value('url', response.url)
        item_loader.add_value('language_code', 'no')
        item_loader.add_value('location_code', 'no')
        item_loader.add_value('site_name', self.allowed_domains[0])
        item_loader.add_value('site_url', self.start_urls[0])
        item_loader.add_value('id', string_to_md5(response.url))

        data_item = item_loader.load_item()
        data_item = dict(data_item)

        self.logger.debug("Data dict: {}".format(Json.dumps(data_item, indent=4)))
        return data_item

    @staticmethod
    def get_abstract(content):
        contents = content.split()
        res = ''
        window = 13
        for i in range(min(len(contents), window)):
            res = res + " " + contents[i]
        return res.strip()

    @staticmethod
    def get_job_info(response):

        info = dict()
        info['employer_address'] = {}

        contents = response.css('section.panel dl.definition-list')
        for content in contents:
            soup = BeautifulSoup(content.get(), 'html.parser')
            key = ''
            for item in soup.find_all(['dt', 'dd']):
                if item.name == 'dt':
                    key = item.text
                else:
                    info[key] = str(info.get(key, '') + ' ' + item.text.strip()).strip()

        info['ad_expires'] = fix_time_format(info.pop('Frist', None))
        info['ad_source'] = info.pop('Nettverk', None)
        info['job_title'] = info.pop('Stillingstittel', None)
        info['job_category'] = info.pop('Søknad merkes', None)
        info['job_function'] = info.pop('Stillingsfunksjon', None)
        info['open_positions'] = info.pop('Antall stillinger', None)
        info['employer_name'] = info.pop('Arbeidsgiver', None)
        info['employer_type'] = info.pop('Sektor', None)
        info['employer_category'] = info.pop('Bransje', None)
        info['employer_address']['address'] = info.pop('Sted', None)
        info.pop('Varighet', None)
        info.pop('Tiltredelse', None)

        info = dict(filter(itemgetter(1), info.items()))

        return info

    @staticmethod
    def get_created_time(soup_ibj):
        values = soup_ibj.findAll('td', {'class': 'u-pl16'})
        values = values[1].getText() if len(values) > 1 else ""
        date = fix_time_format(values)
        return date

    @staticmethod
    def parse_content(response):
        # extractor = Extractor(extractor='ArticleExtractor', html=response.text)
        paragraphs = justext.justext(response.text, justext.get_stoplist("Norwegian_Nynorsk"))
        story = ""
        for paragraph in paragraphs:
            if not paragraph.is_boilerplate:
                story += " " + paragraph.text

                if not story.endswith("."):
                    story += "."

        return story.strip()

    @staticmethod
    def is_leaf_node(url):
        if url is None:
            return False

        url = url.lower()
        url_segments = list(filter(None, url.split('/')))

        # detection logic (based on url)
        logic_key = 'job'
        if logic_key in url_segments:
            if url_segments[-1].startswith('ad.html?'):
                return True

        return False

    def close(self, reason):
        start_time = self.crawler.stats.get_value('start_time')
        finish_time = self.crawler.stats.get_value('finish_time')
        response_time = finish_time - start_time

        LogPrinter('finn.no', 'NO', response_time, self.tentative_jobs_count, self.scraped_jobs_count).print()
        