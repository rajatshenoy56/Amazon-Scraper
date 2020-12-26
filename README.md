# Amazon-Scraper
An Amazon Scraper written in Scrapy framework, deployed using scrapyd, and the results stored in ElasticSearch.

Steps:

1. pip install -r requirements.txt

2. pip install Twisted-20.3.0-cp39-cp39-win_amd64.whl

3. Start a scrapyd instance in one terminal with "scrapyd"

5. Get to where your actual project is "cd ecomm"

4. In another terminal deploy your spider with "scrapyd-deploy local"

5. Setup ElasticSearch and Kibana

5. cd .. and python script.py
