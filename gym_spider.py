import scrapy
import json
import pandas as pd
from scrapy.crawler import CrawlerProcess
import os


class MindbodySpider(scrapy.Spider):
    name = 'mindbody_spider'

    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 3.2,
    }

    # NOTE: The placeholder for the page number is <<num>>
    starting_payload = '{' \
                       '"sort":"-_score,distance",' \
                       '"page":{"size":50,"number":<<num>>},' \
                       '"filter":{"categories":"any","latitude":<<lat>>,"longitude":<<lon>>,"categoryTypes":"any"}' \
                       '}'


    headers = {
        "cookie": "__cf_bm=zdIhLHXKd2OAveBChKORUMdydUFVzC2Ma51sQxv.UJ0-1694646164-0-Abmbwcj2wNw%2FpityY4DWRWy%2FftBkjTO0vQ3tZ0gwU0P5bsTqcasf2XZlBwL%2BUaevGaH%2BTDzZOJPBXbWYwgsXkJc%3D",
        "authority": "prod-mkt-gateway.mindbody.io",
        "accept": "application/vnd.api+json",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.mindbodyonline.com",
        "sec-ch-ua": "^\^Not/A",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "^\^Windows^^",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "x-mb-app-build": "2023-08-02T13:33:44.200Z",
        "x-mb-app-name": "mindbody.io",
        "x-mb-app-version": "e5d1fad6",
        "x-mb-user-session-id": "oeu1688920580338r0.2065068094427127"
    }

    def __init__(self):
        scrapy.Spider.__init__(self)
        self.city_count = 0

    def start_requests(self):
        cities = pd.read_csv('uscities.csv')

        # Using a slice for testing, remove [16001:16005] to scrape all cities
        for idx, city in cities[16001:16005].iterrows():
            lat, lon = city.lat, city.lng
            self.logger.info(f"{city.city}, {city.state_id} started")

            # FIX: Changed '<<pg>>' to '<<num>>'
            payload = self.starting_payload.replace('<<num>>', '1').replace('<<lat>>', str(lat)).replace('<<lon>>', str(lon))

            print(payload)

            yield scrapy.Request(
                url="https://prod-mkt-gateway.mindbody.io/v1/search/locations",
                method="POST", # FIX: Changed to POST
                body=payload,
                headers=self.headers,
                meta={'city_name': city.city, 'page_num': 1, 'lat': lat, 'lon': lon, 'state': city.state_id},
                callback=self.parse
            )

    def parse(self, response):
        data = json.loads(response.text)
        gyms_df = pd.json_normalize(data['data'])

        # Save the dataframe to a CSV
        city_name = response.meta['city_name']
        state = response.meta['state']
        fname = f'{city_name}_{state}.csv'.replace(' ', '_')
        csv_path = f'./data/cities2/{fname}'

        # Check if file exists to determine the write mode
        # NOTE: You will need to manually create the `./data/cities2/` directory before running
        write_mode = 'a' if os.path.exists(csv_path) else 'w'

        # Write data, append if file exists, include header only for a new file
        gyms_df.to_csv(csv_path, mode=write_mode, index=False, header=(not os.path.exists(csv_path)))

        # Check if there's another page and if so, initiate the request
        next_page_num = response.meta['page_num'] + 1
        if next_page_num <= 5:  # Your upper limit of 5 pages per city
            lat, lon = response.meta['lat'], response.meta['lon']

            # FIX 1: Changed '<<pg>>' to '<<num>>'
            # FIX 2: Changed '1' to str(next_page_num) to request the next page
            payload = self.starting_payload.replace('<<num>>', str(next_page_num)).replace('<<lat>>', str(lat)).replace('<<lon>>', str(lon))

            yield scrapy.Request(
                url="https://prod-mkt-gateway.mindbody.io/v1/search/locations",
                method="POST", # FIX: Changed to POST
                body=payload,
                headers=self.headers,
                meta={'city_name': response.meta['city_name'], 'page_num': next_page_num, 'lat': lat, 'lon': lon,
                      'state': state},
                callback=self.parse
            )

        self.city_count += 1
        print(response.meta['city_name'], f'complete ({self.city_count})')
        self.logger.info(f"{response.meta['city_name']}, {response.meta['state']} is complete")

