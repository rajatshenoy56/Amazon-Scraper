import os
import random
from datetime import datetime, timedelta

import pytz
import scrapy

from ..items import Product, ProductPage, ProductReview
from ..elasticdb import ElasticDB


def get_product_between(time_now, time_10_early):
    search_object = {"_source": "reviews_url",
                     "query": {"range": {"created_at": {"gte": time_10_early, "lte": time_now}}}}
    result = ElasticDB.search(ElasticDB.es, 'product_db', search_object)
    result_products = result["hits"]["hits"]
    return result_products


class ReviewScraper(scrapy.Spider):
    name = "review"

    headers_list = [
        # Firefox 77 Mac
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.google.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        },
        # Firefox 77 Windows
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.google.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        },
        # Chrome 83 Mac
        {
            "Connection": "keep-alive",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://www.google.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
        },
        # Chrome 83 Windows
        {
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://www.google.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9"
        }
    ]
    run_id = None

    def start_requests(self):

        self.run_id = ElasticDB.create_run(ElasticDB.es)
        time_now = datetime.now(pytz.timezone("Asia/Calcutta"))
        time_10_early = time_now - timedelta(minutes=int(self.time))
        time_now = datetime.strftime(time_now, "%Y-%m-%dT%H:%M:%S.%f%z")
        time_10_early = datetime.strftime(time_10_early, "%Y-%m-%dT%H:%M:%S.%f%z")

        result_products = get_product_between(time_now, time_10_early)
        print(len(result_products))
        headers = random.choice(self.headers_list)
        for product in result_products:
            yield scrapy.Request(url=product["_source"]["reviews_url"], callback=self.get_review, headers=headers,
                                 meta={'page_num': 0})

    def get_review(self, response):
        stars = ["five", "four", "three", "two", "one"]
        headers = random.choice(self.headers_list)
        for num in stars:
            reviews_url = response.url + "&filterByStar=" + num + "_star"
            yield scrapy.Request(url=reviews_url, callback=self.parse_reviews_page, headers=headers,
                                 meta={'num': num})

        headers = random.choice(self.headers_list)
        if response.meta['page_num'] < 5:
            response.meta['page_num'] += 1
            next_page_url = response.xpath("//li[@class='a-last']").xpath("@href").get()
            final_url = response.urljoin(next_page_url)
            yield scrapy.Request(url=final_url, callback=self.get_review, headers=headers,
                                 meta={'page_num': response.meta['page_num']})

    def parse_reviews_page(self, response):
        product_id = str(response.url[22:].split('/')[2])

        review_href_list = response.xpath(
            '//*[contains(concat( " ", @class, " " ), concat( " ", "a-text-bold", " " ))]').xpath("@href").getall()
        review_ids = []
        for review_href in review_href_list:
            review_ids.append(review_href.split("/")[3])

        reviews = response.xpath(
            '//span[@class="a-size-base review-text review-text-content"]/span//text()').getall()
        now = datetime.now(pytz.timezone("Asia/Calcutta"))
        now = datetime.strftime(now, "%Y-%m-%dT%H:%M:%S.%f%z")
        rating = response.meta['num']

        for review_id, review in zip(review_ids, reviews):
            yield ProductReview(review_id=review_id, run_id=self.run_id, product_id=product_id, reviews=review,
                                created_at=now,
                                rating=rating)
