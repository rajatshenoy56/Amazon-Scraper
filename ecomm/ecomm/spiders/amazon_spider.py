from datetime import datetime
import random

import pytz
import scrapy

from ..items import Product, ProductPage, ProductReview
from ..elasticdb import ElasticDB


def string_to_float(price):
    if price:
        if price.strip():
            price = price.strip()
            price = price[1:]
            price = price.strip()
            price = price.replace(",", "")
            price = float(price)
        else:
            return None
    return price


def search_product_page(product_id):
    search_object = {"query": {"term": {"pid": {"value": product_id}}}, "sort": [{"created_at": {"order": "desc"}}]}
    result = ElasticDB.search(ElasticDB.es, 'response_db', search_object)
    result_text = result["hits"]["hits"][0]["_source"]["product_page"]["response"]
    return result_text


class AmazonScraper(scrapy.Spider):
    name = "amazon"

    no_of_pages = 100
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
    ElasticDB.connect_elasticsearch()

    def start_requests(self):

        if ElasticDB.es is not None:
            headers = random.choice(self.headers_list)
            if self.search == "True":
                start_url = "https://www.amazon.in/s?k="+self.search_item+"&ref=nb_sb_noss"
                print(start_url)
                yield scrapy.Request(url=start_url, callback=self.parse, headers=headers)
            else:
                start_url = "https://www.amazon.in/b?ie=UTF8&node=6308595031"
                yield scrapy.Request(url=start_url, callback=self.extract_heading, headers=headers)
        else:
            print("Could not establish connection")

    def extract_heading(self, response):
        urls = response.xpath("//a[@class='a-link-normal aok-block a-text-normal']").xpath("@href").getall()
        for url in urls:
            final_url = response.urljoin(url)
            headers = random.choice(self.headers_list)
            yield scrapy.Request(url=final_url, callback=self.parse, headers=headers)

    def parse(self, response):
        headers = random.choice(self.headers_list)
        self.no_of_pages -= 1
        products = response.xpath("//a[@class='a-link-normal a-text-normal']").xpath("@href").getall()

        for product in products:
            final_url = response.urljoin(product)
            yield scrapy.Request(url=final_url, callback=self.parse_product_page, headers=headers)

        headers = random.choice(self.headers_list)
        if self.no_of_pages > 0:
            next_page_url = response.xpath("//ul[@class='a-pagination']/li[@class='a-last']/a").xpath("@href").get()
            final_url = response.urljoin(next_page_url)
            yield scrapy.Request(url=final_url, callback=self.parse, headers=headers)

    def parse_product_page(self, response):

        product_id = str(response.url[22:].split('/')[2])
        product_page = {"response": str(response.text)}
        now = datetime.now(pytz.timezone("Asia/Calcutta"))
        now = datetime.strftime(now, "%Y-%m-%dT%H:%M:%S.%f%z")
        #You can remove this if you want to, this just saves the whole html page in the database
        yield ProductPage(pid=product_id, product_page=product_page, created_at=now)

        result_text = search_product_page(product_id)

        sel = scrapy.Selector(text=result_text)

        name = sel.xpath("//span[@id='productTitle']//text()").get() or sel.xpath(
            "//h1[@id='title']//text()").get()

        retail_price = sel.xpath("//span[@class='priceBlockStrikePriceString a-text-strike']//text()").get()
        retail_price = string_to_float(retail_price)

        discounted_price = sel.xpath("//span[@id='priceblock_ourprice']//text()").get()
        discounted_price = string_to_float(discounted_price)

        rating = sel.xpath("//div[@id='averageCustomerReviews_feature_div']").xpath(
            "//span[@class='a-icon-alt']//text()").get()

        category_tree = sel.xpath(
            "//*[(@id = 'wayfinding-breadcrumbs_feature_div')]//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'a-color-tertiary', ' ' ))]//text()").getall()

        if name:
            name = name.strip()

        category = None
        sub_category = None
        if len(category_tree) != 0:
            category = category_tree[0]

        if len(category_tree) > 2:
            sub_category = category_tree[2]

        if rating and type(rating) is not str:
            if rating.strip():
                rating = rating.strip()
                rating = rating[0:3]
                rating = float(rating)
        else:
            rating = None

        reviews_url = sel.xpath('//a[@class="a-link-emphasis a-text-bold"]').xpath('@href').get()
        reviews_url = response.urljoin(reviews_url)

        now = datetime.now(pytz.timezone("Asia/Calcutta"))
        now = datetime.strftime(now, "%Y-%m-%dT%H:%M:%S.%f%z")

        yield Product(name=name, rating=rating, retail_price=retail_price, discounted_price=discounted_price,
                      product_id=product_id, category=category, sub_category=sub_category, category_tree=category_tree,
                      reviews_url=reviews_url, created_at=now)
