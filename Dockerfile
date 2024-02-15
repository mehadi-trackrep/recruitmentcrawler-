FROM python:3.9-slim

WORKDIR /RecruitmentCrawler
ADD requirements.txt /RecruitmentCrawler
RUN pip install -r requirements.txt

ADD . /RecruitmentCrawler

# EXPOSE 80

CMD ["./run.sh"]