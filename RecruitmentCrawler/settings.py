import os
from . import user_agents

BOT_NAME = 'RecruitmentCrawler'

SPIDER_MODULES = ['RecruitmentCrawler.spiders']
NEWSPIDER_MODULE = 'RecruitmentCrawler.spiders'

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS = 16    # maximum concurrent requests: (default: 16)
CONCURRENT_REQUESTS_PER_DOMAIN = 2
REACTOR_THREADPOOL_MAXSIZE = 32
COOKIES_ENABLED = False
CONCURRENT_ITEMS = 32

ROOT_DIR = '/local/directory/'
LOG_LEVEL = 'INFO' 
LOG_ENABLED = True
# DOWNLOAD_DELAY = 1

S3_BUCKET_NAME = 'goava-recruitment-crawl-data'
PATH_SUFFIX = ".recruitment.json"
S3_FOLDER = 'recruitment-raw'

ENV = os.environ.get('ENV', default='prod')

FEED_EXPORT_ENCODING = 'utf-8'
HTTP_PROXY = 'http://127.0.0.1:8123'
USER_AGENT_LIST = user_agents.agents

domin_map = { ## domain to lang mappings
    "finn.no": "no",
    "arbeidsplassen.nav.no": "no",
    "se.indeed.com": "sv",
    "fi.indeed.com": "fi",
    "oikotie.fi": "fi"
}

ITEM_PIPELINES = {
    # 'RecruitmentCrawler.pipelines.RCLocalPipeline': 300,
    'RecruitmentCrawler.pipelines.S3Pipeline': 600
}

DOWNLOADER_MIDDLEWARES = {
    'RecruitmentCrawler.middlewares.RandomUserAgentMiddleware': 400,
    # 'RecruitmentCrawler.middlewares.ProxyMiddleware': 410,
    'RecruitmentCrawler.middlewares.S3DuplicateMiddleware': 500,
    # 'RecruitmentCrawler.middlewares.LocalDuplicateMiddleware': 500,
    # 'RecruitmentCrawler.middlewares.SeleniumMiddleware': 543,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None
}