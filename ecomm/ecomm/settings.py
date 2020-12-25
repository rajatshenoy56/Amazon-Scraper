# Scrapy settings for ecomm project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'ecomm'

SPIDER_MODULES = ['ecomm.spiders']
NEWSPIDER_MODULE = 'ecomm.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
REACTOR_THREADPOOL_MAXSIZE = 128
CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS_PER_DOMAIN = 256
CONCURRENT_REQUESTS_PER_IP = 256

CONCURRENT_ITEMS = 100
# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs


DOWNLOAD_MAXSIZE = 0
DOWNLOAD_TIMEOUT = 100000
DOWNLOAD_DELAY = 0.5
RANDOMIZE_DOWNLOAD_DELAY = True


LOG_LEVEL = 'INFO'


# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False


# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'ecomm.middlewares.EcommSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'ecomm.middlewares.EcommDownloaderMiddleware': 543,
    'ecomm.middlewares.CustomRotatingProxiesMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}

# ROTATING_PROXY_LIST = [
#     '103.20.45.147:8070'
# ]
#
# ROTATING_PROXY_BACKOFF_BASE = 10
# ROTATING_PROXY_BACKOFF_CAP = 20

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'ecomm.pipelines.EcommPipeline': 300,
}

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 5
AUTOTHROTTLE_TARGET_CONCURRENCY = 128

RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 401, 403, 404, 405, 406, 407, 408, 409, 410, 429]

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_S?ECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
