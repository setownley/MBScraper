import json
import os
import scrapy
import pandas as pd


class MindbodySpider(scrapy.Spider):
    name = 'mindbody_spider'

    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 3.2,
    }

    # NOTE: The placeholder for the page number is <<num>>
    starting_payload = (
        '{'
        '"sort":"-_score,distance",'
        '"page":{"size":50,"number":<<num>>},'
        '"filter":{"categories":"any","latitude":<<lat>>,"longitude":<<lon>>,"categoryTypes":"any"}'
        '}'
    )


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
        super().__init__()
        self.city_count = 0

    async def start(self):
        """Generate initial requests using Scrapy's async start hook.

        The previous implementation relied on ``start_requests`` (deprecated
        in Scrapy 2.13+) and sliced the city list far beyond the size of the
        bundled ``uscities.csv`` sample, which resulted in no requests being
        scheduled. This method uses the new API and iterates over all cities
        discovered in the CSV so the spider always emits work.
        """

        cities = self._load_cities()

        for _, city in cities.iterrows():
            lat, lon = city.lat, city.lng
            self.logger.info(f"{city.city}, {city.state_id} started")

            payload = self._build_payload(page_num=1, lat=lat, lon=lon)

            yield scrapy.Request(
                url="https://prod-mkt-gateway.mindbody.io/v1/search/locations",
                method="POST",
                body=payload,
                headers=self.headers,
                meta={'city_name': city.city, 'page_num': 1, 'lat': lat, 'lon': lon, 'state': city.state_id},
                callback=self.parse
            )

    def parse(self, response):
        data = json.loads(response.text)

        if 'data' not in data:
            self.logger.warning(f"No data returned for {response.meta['city_name']}, page {response.meta['page_num']}")
            return

        gyms_df = pd.json_normalize(data['data'])

        # Save the dataframe to a CSV
        city_name = response.meta['city_name']
        state = response.meta['state']
        fname = f'{city_name}_{state}.csv'.replace(' ', '_')
        csv_dir = './data/cities2'
        os.makedirs(csv_dir, exist_ok=True)
        csv_path = f'{csv_dir}/{fname}'

        # Check if file exists to determine the write mode
        # NOTE: You will need to manually create the `./data/cities2/` directory before running
        write_mode = 'a' if os.path.exists(csv_path) else 'w'

        # Write data, append if file exists, include header only for a new file
        gyms_df.to_csv(csv_path, mode=write_mode, index=False, header=(not os.path.exists(csv_path)))

        # Check if there's another page and if so, initiate the request
        next_page_num = response.meta['page_num'] + 1
        if next_page_num <= 5:  # Your upper limit of 5 pages per city
            lat, lon = response.meta['lat'], response.meta['lon']

            payload = self._build_payload(page_num=next_page_num, lat=lat, lon=lon)

            yield scrapy.Request(
                url="https://prod-mkt-gateway.mindbody.io/v1/search/locations",
                method="POST",
                body=payload,
                headers=self.headers,
                meta={'city_name': response.meta['city_name'], 'page_num': next_page_num, 'lat': lat, 'lon': lon,
                      'state': state},
                callback=self.parse
            )

        self.city_count += 1
        print(response.meta['city_name'], f'complete ({self.city_count})')
        self.logger.info(f"{response.meta['city_name']}, {response.meta['state']} is complete")

    def _load_cities(self) -> pd.DataFrame:
        """Load city data from ``uscities.csv``.

        Supports both the full dataset format (with ``city``, ``state_id``,
        ``lat``, ``lng`` columns) and the condensed example format included in
        this repository that uses ``Column``/``Value`` pairs.
        """

        cities = pd.read_csv('uscities.csv')

        expected_columns = ['city', 'state_id', 'lat', 'lng']
        if set(expected_columns).issubset(set(cities.columns)):
            return cities[expected_columns]

        if {'Column', 'Value'}.issubset(set(cities.columns)):
            mapped = dict(zip(cities['Column'], cities['Value']))
            missing = set(expected_columns).difference(mapped)
            if missing:
                raise ValueError(f"uscities.csv missing required fields: {sorted(missing)}")
            return pd.DataFrame([mapped])[expected_columns]

        raise ValueError("uscities.csv must contain city/state_id/lat/lng columns or Column/Value pairs")

    def _build_payload(self, page_num: int, lat: float, lon: float) -> str:
        """Fill the API payload template with request-specific values."""

        payload = self.starting_payload.replace('<<num>>', str(page_num))
        payload = payload.replace('<<lat>>', str(lat)).replace('<<lon>>', str(lon))
        return payload

