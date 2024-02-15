import scrapy
from scrapy.loader.processors import TakeFirst


class RecruitmentCrawlerItem(scrapy.Item):
    language_code = scrapy.Field(
        output_processor=TakeFirst()
    )
    location_code = scrapy.Field(
        output_processor=TakeFirst()
    )
    id = scrapy.Field(
        output_processor=TakeFirst()
    )
    url = scrapy.Field(
        output_processor=TakeFirst()
    )
    site_name = scrapy.Field(
        output_processor=TakeFirst()
    )
    site_url = scrapy.Field(
        output_processor=TakeFirst()
    )
    title = scrapy.Field(
        output_processor=TakeFirst()
    )
    summary = scrapy.Field(
        output_processor=TakeFirst()
    )
    text = scrapy.Field(
        output_processor=TakeFirst()
    )
    extra_info = scrapy.Field(
        output_processor=TakeFirst()
    )
    timestamp = scrapy.Field(   # published time in web
        output_processor=TakeFirst()
    )
    triggers = scrapy.Field()