# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import requests
from rotating_proxies.utils import extract_proxy_hostport
from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from rotating_proxies.middlewares import RotatingProxyMiddleware
from rotating_proxies.expire import Proxies, ProxyState
from twisted.internet import task
import logging

logger = logging.getLogger(__name__)


class CustomProxies(Proxies):

    def engine_started(self):
        """ Create a task for updating proxies every hour """
        self.task = task.LoopingCall(self.update_proxies)
        self.task.start(240, now=True)

    def engine_stopped(self):
        if self.task.running:
            self.task.stop()

    def update_proxies(self):
        proxy_list = requests.get(
            "https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=IN&ssl=all&anonymity=all",
            allow_redirects=True)
        # fetch proxies from wherever you want
        new_proxy_list = proxy_list.content.decode('utf8').split("\r\n")
        for proxy in new_proxy_list:
            self.add(proxy)

    def add(self, proxy):
        """ Add a proxy to the proxy list """
        if proxy in self.proxies:
            logger.warn("Proxy <%s> is already in proxies list" % proxy)
            return

        hostport = extract_proxy_hostport(proxy)
        self.proxies[proxy] = ProxyState()
        self.proxies_by_hostport[hostport] = proxy
        self.unchecked.add(proxy)


class CustomRotatingProxiesMiddleware(RotatingProxyMiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        mw = super(CustomRotatingProxiesMiddleware, cls).from_crawler(crawler)
        # Substitute standart `proxies` object with a custom one
        proxy_list = requests.get(
            "https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=IN&ssl=all&anonymity=all",
            allow_redirects=True)
        # fetch proxies from wherever you want
        new_proxy_list = proxy_list.content.decode('utf8').split("\r\n")

        mw.proxies = CustomProxies(mw.cleanup_proxy_list(new_proxy_list), backoff=mw.proxies.backoff)

        # Connect `proxies` to engine signals in order to start and stop looping task
        crawler.signals.connect(mw.proxies.engine_started, signal=signals.engine_started)
        crawler.signals.connect(mw.proxies.engine_stopped, signal=signals.engine_stopped)
        return mw


class EcommSpiderMiddleware:
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

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
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


class EcommDownloaderMiddleware:
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
