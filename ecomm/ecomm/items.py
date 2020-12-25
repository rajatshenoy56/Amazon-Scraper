# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Product(scrapy.Item):
    # define the fields for your item here like:
    product_id = scrapy.Field()
    name = scrapy.Field()
    retail_price = scrapy.Field()
    discounted_price = scrapy.Field()
    rating = scrapy.Field()
    category = scrapy.Field()
    sub_category = scrapy.Field()
    category_tree = scrapy.Field()
    reviews_url = scrapy.Field()
    created_at = scrapy.Field()


class ProductPage(scrapy.Field):
    created_at = scrapy.Field()
    pid = scrapy.Field()
    product_page = scrapy.Field()


class ProductReview(scrapy.Field):
    review_id = scrapy.Field()
    product_id = scrapy.Field()
    reviews = scrapy.Field()
    created_at = scrapy.Field()
    rating = scrapy.Field()
    run_id = scrapy.Field()


