import os
import boto3
import random
import hashlib
from os.path import join
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from botocore.exceptions import ClientError
from RecruitmentCrawler.settings import domin_map
from scrapy.utils.project import get_project_settings

# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options

settings = get_project_settings()


class RecruitmentcrawlerSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class RecruitmentcrawlerDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class RandomUserAgentMiddleware(object):
    """
    A random user-agent is picked from a list of agent to fake this header.
    """

    def process_request(self, request, spider):
        user_agent = random.choice(settings.get('USER_AGENT_LIST'))
        if user_agent:
            request.headers.setdefault('User-Agent', user_agent)


class ProxyMiddleware(object):
    """
    Sets a proxy for anonymous crawling.
    """

    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTP_PROXY')


# class SeleniumMiddleware(object):
#     DRIVER_PATH = '/usr/bin/chromedriver'
#
#     def __init__(self):
#         options = Options()
#         options.headless = True
#         options.add_argument('--headless')
#         options.add_argument('--no-sandbox')
#         options.add_argument('--disable-gpu')
#         options.add_argument('--disable-dev-shm-usage')
#         # options.add_argument('window-size=1200x600')
#
#         self.driver = webdriver.Chrome(options=options, executable_path=self.DRIVER_PATH)
#         self.driver.implicitly_wait(3)
#         pass
#
#     @staticmethod
#     def valid_for_blocket(url, spider):
#         url_segments = list(filter(None, url.split('/')))
#         url_suffix = url_segments[-1]
#         suffix = url_suffix.split("=")[-1] if 'action' in url_suffix else url_suffix.split("=")[0]
#         suffix = suffix.split("#")[0]
#
#         spider.logger.debug("Blocket job url suffix: {}".format(suffix))
#
#         return suffix.isdigit()
#
#     @staticmethod
#     def is_start_url_blocket(url):
#         if 'lediga-jobb-i-hela-sverige/sida' in url:
#             return True
#         return False
#
#     def selenium_request(self, request, spider):
#         self.driver.get(request.url)
#         body = self.driver.page_source
#         spider.logger.info("##-----> SeleniumRequest URL: {}".format(request.url))
#         return HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)
#
#     def process_request(self, request, spider):
#         spider.logger.debug("Middleware Name: {} URL: {}".format(spider.name, request.url))
#
#         # arbetsformedlingen - logic for requests
#         if spider.name in {"arbetsformedlingen"}:
#             return self.selenium_request(request, spider)
#
#         # blocket - logic for requests
#         if spider.name in {"blocket"}:
#             if self.valid_for_blocket(request.url, spider):
#                 return None
#             elif self.is_start_url_blocket(request.url):
#                 return self.selenium_request(request, spider)
#             else:
#                 raise IgnoreRequest("Spider: {} Not a Job url - Ignoring it! URL: {}".format(spider.name, request.url))
#
#         # respect other url and continue process
#         return None


class S3DuplicateMiddleware(object):

    def __init__(self):
        self.aws_secret = settings.get('AWS_SECRET_ACCESS_KEY'),
        self.aws_access = settings.get('AWS_ACCESS_KEY_ID'),
        self.s3_bucket = settings.get('S3_BUCKET_NAME'),
        self.s3_folder = settings.get('S3_FOLDER'),
        self.path_suffix = settings.get('PATH_SUFFIX')
        self.s3 = boto3.resource('s3', aws_access_key_id=self.aws_access[0],
                                 aws_secret_access_key=self.aws_secret[0])

    def s3_exists(self, path, s3_bucket_name=None):
        if s3_bucket_name is None:
            s3_bucket_name = self.s3_bucket[0]

        _exists, obj = True, None
        try:
            obj = self.s3.Object(s3_bucket_name, path).get()
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                _exists = False
        return _exists

    def get_story_path(self, domain, _id, lang):
        story_path = join(self.s3_folder[0], lang, domain, _id + '.' + lang + self.path_suffix)
        return story_path

    @staticmethod
    def string_to_md5(text):
        m = hashlib.md5()
        m.update(text.encode('utf-8'))
        return m.hexdigest()

    def process_request(self, request, spider):
        url = request.url
        domain = spider.allowed_domains[0]
        if not domain:
            domain = spider.domains[0]        
        lang = domin_map.get(domain, "sv")

        _id = self.string_to_md5(url)
        story_path = self.get_story_path(domain=domain, _id=_id, lang=lang)

        if self.s3_exists(story_path):
            spider.logger.info("Exists on s3! Spider: {} ID: {} URL: {}".format(spider.name, _id, url))
            raise IgnoreRequest("Exists on s3! Spider: {} ID: {} URL: {}".format(spider.name, _id, url))

        else:
            spider.logger.info("NEW: {} ID: {} URL: {}".format(spider.name, _id, url))

        return None


class LocalDuplicateMiddleware(object):

    def __init__(self):
        self.root_dir = settings.get('ROOT_DIR')
        self.path_suffix = settings.get('PATH_SUFFIX')

    def get_story_path(self, domain, _id):
        lang = domin_map.get(domain, "sv")
        story_path = join(self.root_dir, lang, domain, _id + "." + lang + self.path_suffix)
        return story_path

    @staticmethod
    def string_to_md5(text):
        m = hashlib.md5()
        m.update(text.encode('utf-8'))
        return m.hexdigest()

    @staticmethod
    def local_exists(path):
        if os.path.exists(path):
            return True
        return False

    def process_request(self, request, spider):
        url = request.url
        domain = spider.allowed_domains[0]
        _id = self.string_to_md5(url)
        story_path = self.get_story_path(domain=domain, _id=_id)

        if self.local_exists(story_path):
            spider.logger.info("Exists on Local! Spider: {} ID: {} URL: {}".format(spider.name, _id, url))
            raise IgnoreRequest("Exists on Local! Spider: {} ID: {} URL: {}".format(spider.name, _id, url))

        return None