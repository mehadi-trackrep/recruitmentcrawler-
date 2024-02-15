# -*- coding: utf-8 -*-
import json

from operator import itemgetter
import datetime

import scrapy
from scrapy.loader import ItemLoader

from RecruitmentCrawler.helpers import string_to_md5, fix_time_format, clean_text, get_required_recruiment_date
from RecruitmentCrawler.items import RecruitmentCrawlerItem
from RecruitmentCrawler.utils import LogPrinter

from loguru import logger as LOG


class ArbetsformedlingenSVSpiderV2(scrapy.Spider):
    name = "arbetsformedlingen_se_v2"
    allowed_domains = ['arbetsformedlingen.se']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'HTTPERROR_ALLOW_ALL': True
    }

    headers = {
        'Content-Type': "application/json"
    }

    # payload = {
    #     "franPubliceringsdatum": "",
    #     "matchningsprofil": {
    #         "profilkriterier": [
    #             {
    #                 "varde": "**",
    #                 "namn": "**",
    #                 "typ": "FRITEXT"
    #             }
    #         ]
    #     },
    #     "sorteringsordning": "DATUM",
    #     "startrad": 0,
    #     "maxAntal": 50
    # }
    payload = {
      "filters": [],
      "fromDate": None,
      "order": "date",
      "maxRecords": 50,
      "startIndex": 0,
      # "toDate": "2022-07-18T08:05:33.596Z",
      "toDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"Z",
      "source": "pb"
    }

    def __init__(self, tentative_jobs_count: int = 0, scraped_jobs_count: int = 0, name: str = None, **kwargs):
        super(ArbetsformedlingenSVSpiderV2, self).__init__(name, **kwargs)
        self.tentative_jobs_count = tentative_jobs_count
        self.scraped_jobs_count = scraped_jobs_count
        self.REQUIRED_RECRUITMENT_DATE = get_required_recruiment_date()        

    def start_requests(self):
        url = "https://platsbanken-api.arbetsformedlingen.se/jobs/v1/search"
        updated_payload = self.payload
        for i in range(0, 2001, 50):
            updated_payload['startIndex'] = i

            yield scrapy.Request(
                url=url,
                method="POST",
                body=json.dumps(updated_payload, ensure_ascii=False),
                headers=self.headers,
                callback=self.hit_get_info
            )

    @staticmethod
    def convert_to_datetime(date_string):
        return datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')

    def hit_get_info(self, response):
        base_url = 'https://platsbanken-api.arbetsformedlingen.se/jobs/v1/job/{}'
        result = json.loads(response.text)

        items = result['ads']

        for item in items:
            PUBLICATION_DATE = self.convert_to_datetime(item.get('publishedDate', ''))
            if datetime.date.today() < PUBLICATION_DATE.date():
                continue
            elif (datetime.date.today() - PUBLICATION_DATE.date()).days > 2:
                break

            fetch_url = base_url.format(item["id"])
            
            self.tentative_jobs_count += 1
            yield scrapy.Request(url=fetch_url, method="GET", headers=self.headers, callback=self.populate_items)
            self.scraped_jobs_count += 1

    def populate_items(self, response):
        item_loader = ItemLoader(item=RecruitmentCrawlerItem())  ###mmh49 Here, itemLoader & Item class connected!
        result = json.loads(response.text)

        # self.logger.info(response.text)

        full_text = clean_text(result["description"])

        item_loader.add_value('id', string_to_md5(response.url))
        item_loader.add_value('title', result['title'])
        item_loader.add_value('summary', full_text[:150])
        item_loader.add_value('text', full_text)
        item_loader.add_value('timestamp', fix_time_format(result.get('publishedDate', "")))
        item_loader.add_value("extra_info", self.parse_extra_info(result))
        # item_loader.add_value('triggers', 'recruitment')

        temp_id = result.get('id', result.get('application', {}).get('webAddress', ''))
        temp_url = f'https://arbetsformedlingen.se/platsbanken/annonser/{temp_id}'
        item_loader.add_value('url', temp_url)
        
        item_loader.add_value('language_code', 'sv')
        item_loader.add_value('location_code', 'se')
        item_loader.add_value('site_name', 'arbetsformedlingen.se')
        item_loader.add_value('site_url', 'https://arbetsformedlingen.se')
        #
        data_item = item_loader.load_item()
        data_item = dict(data_item)

        # self.logger.debug("Data dict: {}".format(Json.dumps(data_item, indent=4)))

        return data_item

    @staticmethod
    def parse_extra_info(result: dict):
        extra_info = dict()
        company = result.get('company', {})
        extra_info['ad_expires'] = fix_time_format(result['lastApplicationDate'])
        extra_info['employer_name'] = company.get('name')
        extra_info['employer_orgno'] = company.get('organisationNumber')
        extra_info['employer_logo'] = result.get('logotype')
        extra_info['employer_website'] = company.get('webAddress')
        extra_info['employer_email'] = company.get('email')
        extra_info['job_title'] = result.get('title')

        extra_info['contact_list'] = list()
        for item in result.get('contacts',[]):
            item["firstname"] = item.get("name")
            item["lastname"] = item.get("surname")
            item["designation"] = item.get("position")
            item["email"] = item.get("email")
            item["mobile_no"] = item.get("mobileNumber")
            item["telephone_no"] = item.get("phoneNumber")
            item["trade_union"] = item.get("union")
            item["description"] = item.get("description")
            extra_info['contact_list'].append(item)

        extra_info['employer_address'] = {}

        extra_info['employer_address']['address'] = company.get('streetAddress')
        extra_info['employer_address']['postal_code'] = result.get('postCode')
        extra_info['employer_address']['country'] = result.get('workplace', {}).get('country')
        extra_info['employer_address']['city'] = result.get('city')

        extra_info = dict(filter(itemgetter(1), extra_info.items()))

        return extra_info

    def close(self, reason):
        start_time = self.crawler.stats.get_value('start_time')
        finish_time = self.crawler.stats.get_value('finish_time')
        response_time = finish_time - start_time

        LogPrinter('arbetsformedlingen.se', 'SE', response_time, self.tentative_jobs_count, self.scraped_jobs_count).print()
        