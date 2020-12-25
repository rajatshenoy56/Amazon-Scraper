import requests
import time
schedule_url = "http://localhost:6800/schedule.json"

#Pass a search option and search_item option for searching a particular keyword
product_scrapper = requests.post(schedule_url, data={"project": "ecomm", "spider": "amazon", "search": "False"})

# product_scrapper = requests.post(schedule_url, data={"project": "ecomm", "spider": "amazon", "search": "True", "search_item":"pea+protein"})
for i in range(50):
    time.sleep(600)
    review_scrapper = requests.post(schedule_url, data={"project": "ecomm", "spider": "review", "time": "15"})

