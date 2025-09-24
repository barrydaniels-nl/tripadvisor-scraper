import requests
import os
from fake_useragent import UserAgent
import dotenv

dotenv.load_dotenv()

class SpiderAPI:
    def __init__(self):
        self.api_key = os.getenv("SPIDER_API_KEY")
        self.ua = UserAgent(platforms='desktop')
        if not self.api_key:
            raise ValueError("SPIDER_API_KEY environment variable is not set.")

    def request_spider_api(self, profile: str, url: str) -> dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        json_data = {
            "url": url,
            "request": "http",
            "user_agent": self.ua.random,
            "cache": True,
            "block_images": False,
            "block_ads": False,
            "locale": "en_US",
        }

        if profile == "basic":
            json_data.update(
                {
                    "return_format": "markdown",
                    "metadata": False,
                    "full_resources": False,
                }
            )
        elif profile == "links":
            json_data.update(
                {
                    "return_format": "markdown",
                    "metadata": False,
                    "return_page_links": True,
                    "full_resources": False,
                }
            )
        elif profile == "detailed":
            json_data.update(
                {
                    "return_format": "markdown",
                    "metadata": True,
                    "return_json_data": True,
                    "return_headers": True,
                    "return_page_links": True,
                    "full_resources": True,
                }
            )
        elif profile == "raw":
            json_data.update(
                {
                    "return_format": "raw",
                    "metadata": False,
                    "return_headers": True,
                    "return_page_links": True,
                    "full_resources": True,
                }
            )
        elif profile == "results":
            json_data.update(
                {
                    "metadata": True,
                    "return_format": "bytes",
                    "return_json_data": True,
                    "return_headers": True,
                    "return_page_links": True,
                    "full_resources": True,
                }
            )
        elif profile == "restaurant":
            json_data.update(
                {
                    "metadata": True,
                    "return_format": "bytes",
                    "return_json_data": True,
                    "return_headers": True,
                    "return_page_links": True,
                    "full_resources": True,
                }
            )


        if os.getenv("PROXY_URL"):
            json_data["remote_proxy"] = os.getenv("PROXY_URL")
        else:
            json_data["proxy"] = "residential"

        response = requests.post(
            "https://api.spider.cloud/scrape", headers=headers, json=json_data
        )

        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
