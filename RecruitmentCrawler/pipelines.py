import os
import boto3
from os.path import join, exists, dirname
from RecruitmentCrawler.helpers import Json
from RecruitmentCrawler.settings import PATH_SUFFIX


class RCLocalPipeline(object):

    def __init__(self, root_dir):
        self.root_dir = root_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_dir=crawler.settings.get('ROOT_DIR')
        )

    def process_item(self, item, spider):
        story_path = self.root_dir
        story_path = join(story_path, item.get('language_code'))
        story_path = join(story_path, item.get('site_name'))
        story_path = join(story_path, item.get('id') + "." + item.get('language_code') + PATH_SUFFIX)

        if not exists(dirname(story_path)):
            os.makedirs(dirname(story_path))

        spider.logger.debug("Story Path: {}".format(story_path))
        spider.logger.debug("Story: {}".format(Json.dumps(dict(item), indent=2)))
        with open(story_path, 'w') as writer:
            writer.write(Json.dumps(dict(item)))
        return item


class S3Pipeline(object):

    def __init__(self, aws_access, aws_secret, s3_bucket, s3_folder, path_suffix):
        self.aws_access = aws_access
        self.aws_secret = aws_secret
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.path_suffix = path_suffix
        self.s3 = boto3.resource('s3', aws_access_key_id=self.aws_access,
                                 aws_secret_access_key=self.aws_secret)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            aws_secret=crawler.settings.get('AWS_SECRET_ACCESS_KEY'),
            aws_access=crawler.settings.get('AWS_ACCESS_KEY_ID'),
            s3_bucket=crawler.settings.get('S3_BUCKET_NAME'),
            s3_folder=crawler.settings.get('S3_FOLDER'),
            path_suffix=crawler.settings.get('PATH_SUFFIX')
        )

    def save_object_to_s3(self, path, obj):
        body = Json.dumps(obj)
        return self.s3.Bucket(self.s3_bucket).put_object(Key=path, Body=body)

    def process_item(self, item, spider):
        story_path = self.s3_folder
        story_path = join(story_path, item.get('language_code'))
        story_path = join(story_path, item.get('site_name'))
        story_path = join(story_path, item.get('id') + "." + item.get('language_code') + self.path_suffix)

        spider.logger.info("Story Path: {}".format(story_path))
        spider.logger.debug("Story: {}".format(Json.dumps(dict(item), indent=2)))

        # save data to s3 (without looking is it exists)
        self.save_object_to_s3(story_path, item)

        return item


class S3LogWriter:
    LOGGER_ROOT_PATH = 'recruitment-logger'

    def __init__(self, aws_access, aws_secret, s3_bucket):
        self.aws_access = aws_access
        self.aws_secret = aws_secret
        self.s3_bucket = s3_bucket
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=self.aws_access,
            aws_secret_access_key=self.aws_secret
        )

    def save_object_to_s3(self, path, obj):
        body = Json.dumps(obj)
        return self.s3.Bucket(self.s3_bucket).put_object(Key=path, Body=body)

    def write_log(self, spider_name, body):
        story_path = self.LOGGER_ROOT_PATH
        story_path = join(story_path, spider_name + ".log")
        self.save_object_to_s3(story_path, body)