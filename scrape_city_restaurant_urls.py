import requests
import os
import fake_useragent as ua
import dotenv
import json

dotenv.load_dotenv()


def get_city_restaurant_url() -> tuple:
    """Fetches the TripAdvisor restaurant URL and results for a city.

    Args:
        city (str): The name of the city to fetch the restaurant URL for.

    Returns:
        tuple: A tuple containing the TripAdvisor restaurant URL and the number of results.
    """

    url = "http://127.0.0.1:8000/api/cities/?never_scraped=true&limit=1"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data[0]:
            tripadvisor_restaurants_url = (
                data[0]["tripadvisor_restaurants_url"]
                if data[0]["tripadvisor_restaurants_url"]
                else None
            )
            tripadvisor_restaurants_results = (
                data[0]["tripadvisor_restaurants_results"]
                if "tripadvisor_restaurants_results" in data[0]
                else None
            )
            return (tripadvisor_restaurants_url, tripadvisor_restaurants_results)
    return (None, None)


def request_spider_api(url: str) -> dict:

    headers = {
        "Authorization": f'Bearer {os.getenv("SPIDER_API_KEY")}',
        "Content-Type": "application/json",
    }

    json_data = {
        "limit": 1,
        "return_format": "markdown",
        "url": url[0],
        "request": "http",
        "metadata": True,
        "user_agent": ua.UserAgent().random,
        "return_json_data": True,
        "return_headers": True,
        "return_page_links": True,
        "full_resources": True,
        # "remote_proxy": os.getenv("PROXY_URL"),
        "proxy": "residential",
        "cache": True,
        "block_images": True,
        "block_ads": False,
        "locale": "en_US",
    }

    print("")

    response = requests.post(
        "https://api.spider.cloud/scrape", headers=headers, json=json_data
    )

    return response.json()


def add_restaurant_basic_info(data: dict) -> dict:
    """Adds basic restaurant information to the data dictionary.

    Args:
        data (dict): The data dictionary to which the restaurant information will be added.
        {'item': {'image': ['https://dynamic-media-cdn.tripadvisor.com/media/photo-o/2c/79/2d/39/hestia-restaurant.jpg'], 'address': {'postalCode': '75005', 'streetAddress': '8 Rue De La Huchette, 75005 Paris France', 'addressRegion': '', 'addressCountry': 'France', '@type': 'PostalAddress', 'addressLocality': ''}, 'aggregateRating': {'reviewCount': 103, 'ratingValue': '4.9', '@type': 'AggregateRating'}, 'description': '', '@type': 'Restaurant', 'priceRange': '$$ - $$$', 'name': 'Hestia', 'telephone': '+33 1 84 74 95 20', 'url': 'https://www.tripadvisor.com/Restaurant_Review-g187147-d27533751-Reviews-Hestia-Paris_Ile_de_France.html?m=68753'}, 'position': 15, '@type': 'ListItem'}

    Returns:
        dict: The updated data dictionary with restaurant information.
    """
    if data:
        payload = {
            "name": data.get("name", ""),
            "tripadvisor_detail_page": data.get("url", ""),
            "address_string": data.get("address", {}).get("streetAddress", ""),
            "postal_code": data.get("address", {}).get("postalCode", ""),
            "city": data.get("address", {}).get("addressLocality", ""),
            "country": data.get("address", {}).get("addressCountry", ""),
            "rating": data.get("ratingValue", ""),
            "num_rieviews": data.get("aggregateRating", {}).get("reviewCount", ""),
            "price_range": data.get("priceRange", ""),
            "telephone": data.get("telephone", ""),
            "image_urls": [data.get("image", [""])[0]],
        }

        url = "http://127.0.0.1:8000/api/restaurants/"

        headers = {
            "Content-Type": "application/json",
        }

        payload = json.dumps(payload)
        response = requests.post(url, json=payload, headers=headers)

        response.raise_for_status()  # Raise an error for bad responses

        if response.status_code == 201:
            print(f"Restaurant {data.get('name', '')} added successfully.")
        else:
            print(
                f"Failed to add restaurant {data.get('name', '')}. Status code: {response.status_code}"
            )


def parse_restaurant_urls(data: str) -> list:
    restaurant_urls = []
    for line in data.splitlines():
        if "/Restaurant_Review-" in line:
            url = line.split('"')[1]
            restaurant_urls.append(url)
    return restaurant_urls


def write_to_file(filename: str, data: list) -> None:
    with open(filename, "w") as file:
        for item in data:
            file.write(f"{item}\n")
    print(f"Data written to {filename}")


if __name__ == "__main__":
    url = get_city_restaurant_url()
    result = request_spider_api(url)
    write_to_file("restaurant_urls.txt", result)

    for item in result[0]["json_data"]["other_scripts"]:
        if "itemListOrder" in item:
            for list_item in item["itemListElement"]:
                print(list_item)
                print("")
