# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .elasticdb import ElasticDB
from .items import Product, ProductPage, ProductReview


class EcommPipeline:

    def process_item(self, item, spider):
        if isinstance(item, Product):
            return self.handle_product(item)
        if isinstance(item, ProductPage):
            return self.handle_product_page(item)
        if isinstance(item, ProductReview):
            return self.handle_product_review(item)

    def handle_product(self, item):
        if item:
            if ElasticDB.es is not None:
                settings = {
                    "mappings": {
                        "properties": {
                            "name": {
                                "type": "text"
                            },
                            "category": {
                                "type": "keyword"
                            },
                            "subcategory": {
                                "type": "keyword"
                            },
                            "category_tree": {
                                "type": "keyword",
                                "index": "false"
                            },
                            "rating": {
                                "type": "float"
                            },
                            "retail_price": {
                                "type": "float"
                            },
                            "discounted_price": {
                                "type": "float"
                            },
                            "product_id": {
                                "type": "keyword"
                            }
                        }
                    }
                }
                if ElasticDB.create_index(ElasticDB.es, settings, 'product_db'):
                    search_object = {"query": {"term": {"product_id": {"value": item['product_id']}}}}
                    result = ElasticDB.search(ElasticDB.es, 'product_db', search_object)

                    if result["hits"]["total"]["value"] > 0:
                        out = ElasticDB.store_record(ElasticDB.es, 'product_db', ItemAdapter(item).asdict(),
                                                     result["hits"]["hits"][0]["_id"])
                        print('Product Data updated')
                    else:
                        out = ElasticDB.store_record(ElasticDB.es, 'product_db', ItemAdapter(item).asdict(), None)
        return item

    def handle_product_page(self, item):
        if item:
            if ElasticDB.es is not None:
                settings = {
                    "settings": {
                        "index": {
                            "sort.field": "created_at",
                            "sort.order": "desc"
                        }
                    },
                    "mappings": {
                        "properties": {
                            "pid": {
                                "type": "keyword"
                            },
                            "product_page": {
                                "type": "object",
                                "enabled": "false"
                            },
                            "created_at": {
                                "type": "date",
                                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"
                            }
                        }
                    }
                }
                if ElasticDB.create_index(ElasticDB.es, settings, 'response_db'):
                    out = ElasticDB.store_record(ElasticDB.es, 'response_db', ItemAdapter(item).asdict(), None)
        return item

    def handle_product_review(self, item):
        if item:
            if ElasticDB.es is not None:
                settings = {
                    "settings": {
                        "index": {
                            "sort.field": "created_at",
                            "sort.order": "desc"
                        }
                    },
                    "mappings": {
                        "properties": {
                            "run_id": {
                                "type": "keyword"
                            },
                            "product_id": {
                                "type": "keyword"
                            },
                            "reviews": {
                                "type": "text",
                            },
                            "created_at": {
                                "type": "date",
                                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"
                            },
                            "rating": {
                                "type": "keyword"
                            },
                            "review_id": {
                                "type": "keyword"
                            }
                        }
                    }
                }
                if ElasticDB.create_index(ElasticDB.es, settings, 'reviews_db'):
                    search_object = {"query": {"term": {"review_id": {"value": item['review_id']}}}}
                    result = ElasticDB.search(ElasticDB.es, 'reviews_db', search_object)

                    if result["hits"]["total"]["value"] > 0:
                        out = ElasticDB.store_record(ElasticDB.es, 'reviews_db', ItemAdapter(item).asdict(),
                                                     result["hits"]["hits"][0]["_id"])
                        print('Review Data updated')
                    else:
                        out = ElasticDB.store_record(ElasticDB.es, 'reviews_db', ItemAdapter(item).asdict(), None)
        return item
