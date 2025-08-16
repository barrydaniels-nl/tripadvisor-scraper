from spider_cloud import SpiderAPI
import requests
import dotenv
import re
import sqlite3
import urllib.parse

dotenv.load_dotenv()

DATABASE_FILE = "city_restaurant_links.db"

client = SpiderAPI()


def get_empty_results_city():
    """Fetches the first city with no TripAdvisor restaurant URL or results."""
    url = "http://127.0.0.1:8000/api/cities/search/?restaurants_is_null=true&never_scraped=True"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data
        else:
            raise ValueError("No Cities with null results found")


def parse_results_number(city_data: dict) -> int:
    """Filters out the number of results from the city data."""

    decoded = ''.join(chr(c) for c in city_data[0]['content'])
    matches = re.findall(r'reviews of ([\d,]+)', decoded)
    numbers = [int(match.replace(',', '')) for match in matches]

    if len(numbers) > 0:
        return int(max(numbers))
    return None


def generate_urls(city, results):
    """Generates the TripAdvisor restaurant URL for a given city."""
    if not city or not results:
        return None

    geo_id = city.get("tripadvisor_geo_id")
    pages = results // 30 + (1 if results % 30 > 0 else 0)
    all_urls = []

    base_url = (
        "https://www.tripadvisor.com/FindRestaurants"
        f"?geo={geo_id}"
        f"&establishmentTypes=10591,11776,12208,16548,16556,9900,9901,9909,21908"
        f"&minimumTravelerRating=TRAVELER_RATING_LOW"
        f"&broadened=false"
    )

    all_urls.append(base_url)

    if pages > 1:
        all_urls.extend([f"{base_url}&offset={i*30}" for i in range(1, pages + 1)])

    return all_urls


def update_city_results_number(geoname_id: int, results: int) -> bool:
    """Main function to update city restaurant URLs and results."""
    url = f"http://127.0.0.1:8000//api/cities/{geoname_id}/update-tripadvisor-results/"

    payload = {
        "tripadvisor_restaurants_results": results,
    }

    response = requests.patch(url, json=payload)

    if response.status_code == 200:
        return True
    else:
        print(f"Failed to update city results: {response.status_code} - {response.text}")
        return False


def add_links_to_database(geoname_id: int, links: list) -> bool:
    """Adds generated links to the database for a given city."""
    if not links or not geoname_id:
        raise ValueError("Links or geoname_id cannot be empty.")
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        for link in links:
            cursor.execute(
                "INSERT INTO city_restaurant_links (geoname_id, url, status) VALUES (?, ?, ?)",
                (geoname_id, link, "pending"),
            )
        
        conn.commit()
        conn.close()
        return True
    
    except Exception as e:
        print(f"Error adding links to database: {e}")
        return False


def main():
    """Main function to update city restaurant URLs and results."""

    for city in get_empty_results_city():
        tripadvisor_geo_id = city.get("tripadvisor_geo_id")
        tripadvisor_restaurants_results = city.get("tripadvisor_restaurants_results")
        geoname_id = city.get("geoname_id")

        if not tripadvisor_geo_id or tripadvisor_restaurants_results or not geoname_id:
            print(f"Skipping city {city['name']} due to missing data.")
            continue
        else:
            print(f"Analyzing city {city['name']}.")

            tripadvisor_restaurants_url = (
                "https://www.tripadvisor.com/FindRestaurants"
                f"?geo={tripadvisor_geo_id}"
                f"&establishmentTypes=10591,11776,12208,16548,16556,9900,9901,9909,21908"
                f"&minimumTravelerRating=TRAVELER_RATING_LOW"
                f"&broadened=false"
            )

            tripadvisor_city_data = client.request_spider_api(
                "results", tripadvisor_restaurants_url
            )
            
            if tripadvisor_city_data[0]['status'] != 200:
                print(f"Failed to fetch data for {city['name']}: {tripadvisor_city_data['message']}")
                continue
            
            results = parse_results_number(tripadvisor_city_data)
            
            if results is not None:
                print(f"Found {results} results for {city['name']} with geoname_id: {city.get('geoname_id')}.")
                
                if update_city_results_number(geoname_id, results):
                    print(f"Successfully updated {city['name']} with {results} results.")
                else:
                    print(f"Failed to update {city['name']} results.")
                
                if links := generate_urls(city, results):
                    if add_links_to_database(geoname_id, links):
                        print(f"Links added to database for {city.get('name')}.")
                    else:
                        print(f"Failed to add links to database for {city.get('name')}.")
                else:
                    print(f"Failed generatting links for {city['name']}.")
            else:
                print(f"No results found for {city.get('name')} with geoname_id: {geoname_id}.")
                    
if __name__ == "__main__":
    main()