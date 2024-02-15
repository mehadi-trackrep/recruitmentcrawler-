import dateutil.parser as dp
import base64
import requests
import datetime

import scrapy
from inline_requests import inline_requests
from scrapy.exceptions import IgnoreRequest
from scrapy.loader import ItemLoader
from scrapy.exceptions import IgnoreRequest

from RecruitmentCrawler.items import RecruitmentCrawlerItem
from RecruitmentCrawler import helpers as Helpers
from RecruitmentCrawler.utils import LogPrinter

from lxml import html
from loguru import logger as LOGGER


class OikotieFISpider(scrapy.Spider):
    name = "oikotie_fi"
    allowed_domains = ['oikotie.fi']

    start_urls = [
        'https://tyopaikat.oikotie.fi/tyopaikat'
    ]

    def __init__(self, tentative_jobs_count: int = 0, scraped_jobs_count: int = 0, name: str = None, **kwargs):
        super(OikotieFISpider, self).__init__(name, **kwargs)
        self.REQUIRED_RECRUITMENT_DATE = Helpers.get_required_recruiment_date()
        self.tentative_jobs_count = tentative_jobs_count
        self.scraped_jobs_count = scraped_jobs_count

    @inline_requests
    def parse(self, response):
        BEFORE_PUBLICATION_DATE = datetime.datetime.utcnow().strftime("%Y-%m-%dT00:00:01.000Z")
        BEFORE_JOB_NO = "0"
        OIKOTIE_FI = 'https://tyopaikat.oikotie.fi'

        FLAG = True
        while FLAG:
            try:
                all_data = self.get_data(
                    BEFORE_PUBLICATION_DATE, BEFORE_JOB_NO)
                if all_data.get('data') and len(all_data['data']['jobAdSearchV2']['edges']) == 0:
                    break
                    
                for index, element in enumerate(all_data['data']['jobAdSearchV2']['edges']):
                    PUBLICATION_DATE = dp.parse(
                        str(element['node']['publicationDate'])).date()
                    if self.REQUIRED_RECRUITMENT_DATE == PUBLICATION_DATE:
                        job_url = f'{OIKOTIE_FI}{element["node"]["jobAdPath"]}'
                        description = self.get_description(job_url)
                        recruitment_obj = self.create_recruitment_object(
                            element, job_url, description)

                        self.tentative_jobs_count += 1

                        yield self.populate_items(result=recruitment_obj)

                        self.scraped_jobs_count += 1
                    elif self.REQUIRED_RECRUITMENT_DATE > PUBLICATION_DATE:
                        FLAG = False
                        break

                    BEFORE_PUBLICATION_DATE = element['node']['publicationDate']
                    BEFORE_JOB_NO = recruitment_obj['url'].split('/')[-1]
            except Exception as e:
                LOGGER.error('{}'.format(str(e)))
                break

    def get_description(self, job_url):
        try:
            headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
            }
            # Using the custom headers we defined above
            response = requests.get(job_url, headers=headers)
            sel_desc = html.fromstring(response.content)
            description = sel_desc.xpath(
                '//div[@id="jobDescription"]/descendant::text()')
            if not description or len(description) == 0:
                description = sel_desc.xpath(
                    '//div[@data-e2e-component="job-ad-description"]/descendant::text()')
                if not description or len(description) == 0:
                    LOGGER.error('{}'.format(job_url))
                    description = ['']

            description = ' '.join(description)
            description = description.replace('\n ', '\n')

        except Exception as e:
            LOGGER.error('{}'.format(str(e)))

            return None

        return description

    def get_data(self, publish_datetime, job_no):
        base64_message = self.get_hash_of_start_point(publish_datetime, job_no)
        API_ENDPOINT_URL = f'https://tapi.tyopaikat.oikotie.fi/graphql?operationName=Search&variables={{"filter":{{"source":"oikotie"}},"sort":"uusimmat","options":[],"first":500,"after":"{base64_message}","getSearchMetaData":false}}&extensions={{"persistedQuery":{{"version":1,"sha256Hash":"35d03197ab42fdcdf3e46bd2159173257be92c911d69785ae30af0b21cafe99d"}}}}'

        try:
            headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
            }
            # Using the custom headers we defined above
            response = requests.get(url=API_ENDPOINT_URL, headers=headers)
        except Exception as e:
            LOGGER.error('{}'.format(str(e)))

            return None

        if response and response.status_code == 200:
            return response.json()

    def create_recruitment_object(self, element, job_url, description):

        obj = {
            'url': Helpers.check_none_type_var(job_url),
            'title': Helpers.check_none_type_var(element['node'].get('name') or None),
            'summary': Helpers.check_none_type_var(' '.join(description.split(' ')[0:30])),
            'text': Helpers.check_none_type_var(description),
            'company_name': Helpers.check_none_type_var(element['node'].get("employer") or None),
            'contract_type': Helpers.check_none_type_var(element['node'].get('contractType') or None),
            'job_type': Helpers.check_none_type_var(element['node']['attributes'].get('isRemoteWork') or None),
            'job_key': Helpers.check_none_type_var(element['node'].get("jobAdPath") or None).split('/')[-1],
            'experience_level': Helpers.check_none_type_var(element['node'].get('experienceLevel') or None),
            'publication_date': Helpers.check_none_type_var(element['node'].get('publicationDate') or None)[:-5],
            'application_deadline': Helpers.check_none_type_var(element['node'].get('lastVisibleDate') or None)[:-5],
            'location': ' '.join([Helpers.check_none_type_var(city_name.get('cityName') or None) for city_name in element['node']['locations']])
        }

        return obj

    def get_hash_of_start_point(self, publish_datetime, job_no):
        iso_timestamp = self.extract_iso_timestamp(publish_datetime)

        message = f'[{iso_timestamp},"{job_no}"]'
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')

        return base64_message

    def extract_iso_timestamp(self, publishStartDate):
        parsed_t = dp.parse(publishStartDate)
        t_in_seconds = parsed_t.timestamp()
        t_in_seconds = int(t_in_seconds)
        t_in_seconds = str(t_in_seconds)+"000"
        t_in_seconds = int(t_in_seconds)

        return t_in_seconds

    def populate_items(self, result):
        try:
            item_loader = ItemLoader(item=RecruitmentCrawlerItem())

            extra_info = dict()
            extra_info['employer_name'] = result['company_name']
            extra_info['employer_address'] = result['location']
            if result['job_type']:
                extra_info['job_type'] = result['job_type']
            extra_info['job_key'] = result['job_key']
            extra_info['experience_level'] = result['experience_level']
            extra_info['contract_type'] = result['contract_type']
            extra_info['application_deadline'] = Helpers.fix_time_format(
                result['application_deadline'])

            item_loader.add_value('id', Helpers.string_to_md5(result['url']))
            item_loader.add_value('title', result['title'])
            item_loader.add_value('summary', result['summary'])
            # 'text' == 'description'
            item_loader.add_value('text', result['text'])
            item_loader.add_value(
                'timestamp', Helpers.fix_time_format(result['publication_date']))
            item_loader.add_value('extra_info', extra_info)
            item_loader.add_value('url', result['url'])
            item_loader.add_value('language_code', 'fi')
            item_loader.add_value('location_code', 'fi')
            item_loader.add_value('site_name', 'tyopaikat.oikotie.fi')
            item_loader.add_value('site_url', 'https://tyopaikat.oikotie.fi/')

            data_item = item_loader.load_item()
            data_item = dict(data_item)

            return data_item
        except IgnoreRequest:
            pass

    def close(self, reason):
        start_time = self.crawler.stats.get_value('start_time')
        finish_time = self.crawler.stats.get_value('finish_time')
        response_time = finish_time - start_time

        LogPrinter('tyopaikat.oikotie.fi', 'FI', response_time, self.tentative_jobs_count, self.scraped_jobs_count).print()
