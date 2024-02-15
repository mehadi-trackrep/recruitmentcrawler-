
import os
import re
import json
import scrapy
import traceback
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import urlencode
from loguru import logger as LOGGER
from scrapy.selector import Selector
from scrapy.loader import ItemLoader
from scrapy.exceptions import IgnoreRequest
from RecruitmentCrawler.settings import ENV
from RecruitmentCrawler.utils import LogPrinter
from RecruitmentCrawler.items import RecruitmentCrawlerItem
from RecruitmentCrawler.helpers import string_to_md5, fix_time_format, clean_text

load_dotenv()  # take environment variables from .env.

HTML_TAGS_REMOVAL_REGEX_PATTERN_OBJ = re.compile(r'<[^>]+>')
VIEW_JOB_POSTINGS_BASE_URL = 'https://www.indeed.com/m/basecamp/viewjob?viewtype=embedded&jk='

LANGUAGE_CODE =  'sv'
LOCATION_CODE = 'se'
SITE_NAME = 'se.indeed.com'
SITE_URL = 'https://se.indeed.com/'

class IndeedSVSpider(scrapy.Spider):
    name = "indeed_se"

    custom_settings = {
        'SCRAPEOPS_PROXY_ENABLED': True,
        # Add In The ScrapeOps Extension
        # 'EXTENSIONS': {
        #     'scrapeops_scrapy.extension.ScrapeOpsMonitor': 500, 
        # },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725
        }
    }

    job_items_writer_file = None
    if ENV.upper() in ["LOCAL_TEST", "TEST"]:
        job_items_writer_file = open("basecamp_viewpage.json", "w")

    def __init__(self, tentative_jobs_count: int = 0, scraped_jobs_count: int = 0, name: str = None, **kwargs):
        super(IndeedSVSpider, self).__init__(name, **kwargs)
        self.tentative_jobs_count = tentative_jobs_count
        self.all_jk_codes = set([])
        self.scraped_jobs_count = scraped_jobs_count

    def get_indeed_search_url(self, location, offset=0):
        parameters = {
            "l": location, 
            "radius": 100, 
            "fromage": 1, 
            "start": offset
        }

        d = {
            "api_key": os.getenv('SCRAPEOPS_API_KEY'),
            "url": f"{SITE_URL}jobs?" + urlencode(parameters)
        }

        if ENV.upper() in ["LOCAL_TEST", "TEST"]:
            LOGGER.info(f"\n-->indeed_jobs_url: {d['url']}\n")

        return f"https://proxy.scrapeops.io/v1/?" + urlencode(d)

    def start_requests(self):
        location_list = ['Stockholm', 'Göteborg', 'Malmö', 
                         'Uppsala', 'Helsingborg', 'Västerås',
                         'Jönköping', 'Örebro', 'Linköping',
                         'Norrköping'
        ]

        if ENV.upper() in ["LOCAL_TEST", "TEST"]:
            location_list = ['Linköping']

        for location in location_list:
            indeed_jobs_url = self.get_indeed_search_url(location)
            yield scrapy.Request(
                url=indeed_jobs_url
                ,callback=self.parse_search_results
                ,meta={'location': location, 'offset': 0}
                ## ,timeout=120
            )

    def parse_search_results(self, response):
        
        location = response.meta['location']
        offset = response.meta['offset'] 
        script_tag  = re.findall(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', response.text)

        if script_tag:

            json_blob = {}
            try:
                json_blob = json.loads(script_tag[0])
            except Exception as exc:
                LOGGER.debug("Got an exception, exc: {}".format(traceback.format_exc()))

            ## Paginate Through Jobs Pages
            if offset == 0:
                sel = Selector(text=response.text)
                total_jobcount = sel.xpath('//div[contains(@class, "jobsearch-JobCountAndSortPane-jobCount")]/span/text()').extract()
                
                num_results = 350 ## A THRESHOLD VALUE!!
                try:
                    LOGGER.info("==>total_jobcount: {}".format(total_jobcount))
                    num_results = int(re.findall(r'\d+', total_jobcount[0])[0])
                except Exception as exc:
                    LOGGER.warning(f"Got an exception while trying to find total jobs: {exc}")
                    num_results = 350 ## A THRESHOLD VALUE!!                
                LOGGER.info("==> when offset == 0 then 'total jobs': {}".format(num_results))

                # if num_results > 1000:
                #     num_results = 50
                
                for offset in range(10, num_results + 10, 10):
                    url = self.get_indeed_search_url(location, offset)
                    yield scrapy.Request( ## recursive call!! this if block will be executed only when offset==0, i mean first time
                        url=url, 
                        callback=self.parse_search_results, 
                        meta={'location': location, 'offset': offset}
                    )

            LOGGER.debug(f"############### offset: {offset} ###############")
            
            ## Extract Jobs From Search Page
            jobs_list = json_blob['metaData']['mosaicProviderJobCardsModel']['results']

            current_job_keys = []
            for job in jobs_list:
                if job.get('jobkey'):
                    current_job_keys.append(job.get('jobkey'))
            
            new_job_keys = set(current_job_keys) - self.all_jk_codes
            self.tentative_jobs_count += len(new_job_keys) ## IT'S FOR GOAVA MONITOR!!
            self.all_jk_codes.update(new_job_keys)
            
            for index, job_key in enumerate(list(new_job_keys)): ## iterate through each jk (job keys) to get details
                if ENV.upper() in ["LOCAL_TEST", "TEST"]:
                    LOGGER.debug("INDEX: {}, job_key: {}".format(index, job_key))
                d = {
                    "api_key": os.getenv('SCRAPEOPS_API_KEY'),
                    "url": VIEW_JOB_POSTINGS_BASE_URL + job_key
                }
                job_url = f"https://proxy.scrapeops.io/v1/?" + urlencode(d)
                
                yield scrapy.Request(
                    url=job_url
                    ,callback=self.parse_job
                    ,meta={
                        'location': location, 
                        'page': round(offset / 10) + 1 if offset > 0 else 1,
                        'position': index,
                        'jobKey': job_key
                    }
                )

    def parse_job(self, response): ## this response is for a "single" job posting (/recruitment)
        
        location = response.meta['location']
        page = response.meta['page'] 
        position = response.meta['position'] 


        script_tag  = re.findall(r"_initialData=(\{.+?\});", response.text)

        if script_tag:
            our_required_item = {}
            try:
                json_blob = json.loads(script_tag[0])
                job = json_blob["jobInfoWrapperModel"]["jobInfoModel"]
                if ENV.upper() in ["LOCAL_TEST", "TEST"] and self.job_items_writer_file:
                    self.job_items_writer_file.write(json.dumps(job, indent=4, ensure_ascii=False))
                job_description = self.parse_job_description(job)
                our_required_item = {
                    'jobTitle': job.get('jobInfoHeaderModel').get('jobTitle'),
                    'subTitle': job.get('jobInfoHeaderModel').get('subtitle'),
                    'company': job.get('jobInfoHeaderModel').get('companyName'),
                    'location': job.get('jobInfoHeaderModel').get('formattedLocation', location),
                    'jobType': job.get('jobMetadataHeaderModel').get('jobType'),
                    'companyOverviewLink': job.get('jobInfoHeaderModel').get('companyOverviewLink'),
                    'page': page,
                    'position': position,
                    'jobKey': response.meta['jobKey'],
                    'jobDescription': job_description,
                    'url': VIEW_JOB_POSTINGS_BASE_URL + response.meta['jobKey']
                }
            except Exception as exc:
                LOGGER.exception(f"Got an exception during parsing - exc: {traceback.format_exc()}")
            
            yield self.populate_items(result=our_required_item)

    def populate_items(self, result):
        ## LOGGER.info("====> result(populate_items()): ", result)
        try:
            item_loader = ItemLoader(item=RecruitmentCrawlerItem())

            extra_info = dict()
            if result.get('company'):
                extra_info['employer_name'] = result['company']
            if result.get('location'):
                extra_info['employer_address'] = result['location']
            if result.get('jobType'):
                extra_info['job_type'] = result['jobType']
            if result.get('companyOverviewLink'):
                extra_info['company_overview_link'] = result['companyOverviewLink']
            if result.get('subTitle'):
                extra_info['subtitle'] = result['subTitle']
            if result.get('jobKey'):
                extra_info['job_key'] = result['jobKey']
            if result.get('page'):
                extra_info['page'] = result['page']
            if result.get('position'):
                extra_info['position'] = result['position']

            full_text = clean_text(result.get('jobDescription'))

            item_loader.add_value('id', string_to_md5(result['url']))
            item_loader.add_value('title', result.get('jobTitle'))
            item_loader.add_value('summary', full_text[:150] + '...')
            item_loader.add_value('text', full_text) # 'text' == 'description'
            item_loader.add_value('url', result.get('url'))
            item_loader.add_value('timestamp', fix_time_format(datetime.now()))
            item_loader.add_value('extra_info', extra_info)
            item_loader.add_value('language_code', LANGUAGE_CODE)
            item_loader.add_value('location_code', LOCATION_CODE)
            item_loader.add_value('site_name', SITE_NAME)
            item_loader.add_value('site_url', SITE_URL)

            data_item = item_loader.load_item()
            self.scraped_jobs_count += 1  ## IT'S FOR GOAVA MONITOR!!

            data_item = dict(data_item)
            return data_item
        except IgnoreRequest:
            pass

    def parse_job_description(self, job):
        sanitizedJobDescription = job.get('sanitizedJobDescription')
        if isinstance(sanitizedJobDescription, dict):
            job_description = sanitizedJobDescription.get('content')
        elif isinstance(sanitizedJobDescription, str):
            job_description = sanitizedJobDescription
        else:
            job_description = ''
        job_description = HTML_TAGS_REMOVAL_REGEX_PATTERN_OBJ.sub('', job_description)
        return job_description

    def close(self, reason):
        start_time = self.crawler.stats.get_value('start_time')
        finish_time = self.crawler.stats.get_value('finish_time')
        response_time = finish_time - start_time

        LogPrinter(SITE_NAME, LOCATION_CODE.upper(), response_time, self.tentative_jobs_count, self.scraped_jobs_count).print()
        