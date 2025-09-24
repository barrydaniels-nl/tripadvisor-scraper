import requests
from dotenv import load_dotenv
from loguru import logger
from time import sleep
import os
import json
import time
import argparse
from unidecode import unidecode

load_dotenv()


logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")


def get_non_geoid_cities():

    url = "http://127.0.0.1:8000/api/cities/search/?tripadvisor_geo_id_is_null=true&page_size=250&never_scraped=true"
    all_cities = []

    while True:
        response = requests.get(url)
        if response.status_code == 200:
            for city in response.json()['results']:
                all_cities.append(city)
            
            if response.json()['next']:
                url = response.json()['next']
            else:
                break
        else:
            response.raise_for_status()

    return all_cities


def get_cities_with_geoid():

    url = "http://127.0.0.1:8000/api/cities/search/?tripadvisor_geo_id_is_null=false&page_size=250&country=IT"
    all_cities = []

    while True:
        response = requests.get(url)
        if response.status_code == 200:
            for city in response.json()['results']:
                all_cities.append(city)
            
            if response.json()['next']:
                url = response.json()['next']
            else:
                break
        else:
            response.raise_for_status()

    return all_cities


def check_all_cities_geo_ids():
    cities = get_cities_with_geoid()
    logger.info(f"Found {len(cities)} cities with existing geo IDs to check")
    
    for city in cities:
        sleep(0.3)
        if city.get('region') is not None:
            if region := city['region']['name']:
                city_string = f"{city['name']} {region} {city['country']['name']}"
            else:
                city_string = f"{city['name']} {city['country']['name']}"
        else:
            city_string = f"{city['name']} {city['country']['name']}"

        try:
            logger.info(f"Checking city: {unidecode(city_string)} (GeoName ID: {city['geoname_id']}, Current Geo ID: {city.get('tripadvisor_geo_id')})")
            city_has_correct_geo_id(city, unidecode(city_string))
            update_last_scraped(city['geoname_id'])
        except Exception as e:
            logger.error(f"Error checking city {city_string}: {e}")
            update_last_scraped(city['geoname_id'])
            continue


def get_current_city_geo_id(geoname_id):

    url = f"http://127.0.0.1:8000/api/cities/{geoname_id}/"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get('tripadvisor_geo_id', None)


def update_city_geo_id(geoname_id, geo_id):

    url = f"http://127.0.0.1:8000/api/cities/{geoname_id}/"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    data = {
        "tripadvisor_geo_id": geo_id,
        "tripadvisor_restaurants_url": "",
        "tripadvisor_hotels_url": "",
        "tripadvisor_attractions_url": ""
    }

    response = requests.put(url, json=data, headers=headers)
    if response.status_code == 200:
        logger.info(f"Updated city {geoname_id} with Geo ID {geo_id}")
    else:
        logger.error(f"Failed to update city {geoname_id}: {response.status_code} - {response.text}")
        response.raise_for_status()


def unlink_restaurants_from_city(geoname_id):
    """
    Delete all restaurants linked to a city.
    This is needed when a city's tripadvisor_geo_id changes.
    
    Args:
        geoname_id: The geoname ID of the city whose restaurants should be unlinked
        
    Returns:
        Number of restaurants deleted
    """
    restaurants_url = f"http://127.0.0.1:8000/api/restaurants/search/?geoname_id={geoname_id}&page_size=100000"
    
    try:
        response = requests.get(restaurants_url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch restaurants for geoname_id {geoname_id}: {response.status_code}")
            return 0
            
        restaurants = response.json()['results']
        deleted_count = 0
        
        if not restaurants:
            logger.info(f"No restaurants found for geoname_id {geoname_id}")
            return 0
            
        logger.info(f"Found {len(restaurants)} restaurants to unlink for geoname_id {geoname_id}")
        
        for restaurant in restaurants:
            restaurant_id = restaurant['id']
            delete_url = f"http://127.0.0.1:8000/api/restaurants/{restaurant_id}/"
            
            try:
                delete_response = requests.delete(delete_url)
                
                if delete_response.status_code in [200, 204]:
                    deleted_count += 1
                    logger.debug(f"Deleted restaurant {restaurant_id}: {restaurant.get('name', 'Unknown')}")
                else:
                    logger.warning(f"Failed to delete restaurant {restaurant_id}: {delete_response.status_code}")
                    
            except Exception as e:
                logger.error(f"Error deleting restaurant {restaurant_id}: {e}")
                continue
        
        logger.info(f"Successfully deleted {deleted_count}/{len(restaurants)} restaurants for geoname_id {geoname_id}")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error unlinking restaurants for geoname_id {geoname_id}: {e}")
        return 0


def change_city_geoname_id(old_geoname_id, new_geoname_id):
    """
    1. get all restaurants for old_geoname_id
    2. update each restaurant to new_geoname_id
    """

    restaurants_url = f"http://127.0.0.1:8000/api/restaurants/search/?geoname_id={old_geoname_id}&page_size=100000"
    restaurants = requests.get(restaurants_url).json()['results']
    logger.info(f"Found {len(restaurants)} restaurants for city with old GeoName ID {old_geoname_id}")

    for restaurant in restaurants:
        restaurant_id = restaurant['id']
        update_url = f"http://127.0.0.1:8000/api/restaurants/{restaurant_id}/"
        data = {
            "geoname_id": new_geoname_id
        }
        response = requests.put(update_url, json=data)

        if response.status_code == 200:
            logger.info(f"Updated restaurant {restaurant_id} to new GeoName ID {new_geoname_id}")
        else:
            logger.error(f"Failed to update restaurant {restaurant_id}: {response.status_code} - {response.text}")
            response.raise_for_status()


def update_last_scraped(geoname_id):

    url = f"http://127.0.0.1:8000/api/cities/{geoname_id}/update-scraped/"
    response = requests.patch(url)
    if response.status_code == 200:
        logger.info(f"Updated last_scraped for city {geoname_id}")
        return True
    else:
        logger.error(f"Failed to update last_scraped for city {geoname_id}: {response.status_code} - {response.text}")
        response.raise_for_status()
        return False


def city_has_correct_geo_id(city, city_string):
    current_geo_id = get_current_city_geo_id(city['geoname_id'])
    if current_geo_id is not None:
        new_geo_id = search_city_on_tripadvisor(city, city_string)
        if new_geo_id and new_geo_id != current_geo_id:
            logger.info("-" * 40)
            logger.warning(f"City {city_string} has a different Geo ID {current_geo_id} than found {new_geo_id}, updating.")
            logger.info("-" * 40)
            logger.info(f"old: https://www.tripadvisor.com/findRestaurants?geo={current_geo_id}")
            logger.info(f"new: https://www.tripadvisor.com/findRestaurants?geo={new_geo_id}")
            logger.info(f"map: https://www.google.com/maps/search/?api=1&query={city['latitude']}%2C{city['longitude']}")
            logger.info("-" * 40)

            # Count restaurants that will be affected
            restaurants_url = f"http://127.0.0.1:8000/api/restaurants/search/?geoname_id={city['geoname_id']}&page_size=1"
            try:
                count_response = requests.get(restaurants_url)
                if count_response.status_code == 200:
                    total_count = count_response.json().get('count', 0)
                    if total_count > 0:
                        logger.warning(f"⚠️  This will unlink {total_count} restaurants from the current city")
            except Exception as e:
                logger.debug(f"Could not count restaurants: {e}")
            
            user_input = input("Change the Geo ID? Press Y to confirm...")

            if user_input.strip().lower() == 'y':
                # First unlink all restaurants from the city
                deleted_count = unlink_restaurants_from_city(city['geoname_id'])
                if deleted_count > 0:
                    logger.info(f"Unlinked {deleted_count} restaurants from the city before updating geo_id")
                
                # Then update the city's geo_id
                update_city_geo_id(city['geoname_id'], new_geo_id)
                return True
            else:
                logger.info("Skipped updating Geo ID.")
                return False
        else:
            logger.info(f"City {city_string} has correct Geo ID: {current_geo_id}")
            return True
    else:
        return False


def search_city_on_tripadvisor(city, city_string):
    tripadvisor_api_key = os.getenv("TRIPADVISOR_API_KEY")

    # url for search by name 
    search_url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={tripadvisor_api_key}&category=geos&searchQuery={city_string}"

    # latlong nearby search 
    # latlong = f"{city['latitude']},{city['longitude']}"
    # search_url = f"https://api.content.tripadvisor.com/api/v1/location/nearby_search?latLong={latlong}&key={tripadvisor_api_key}&category=geos&language=en"
    logger.info(f"Searching for city: {city_string}")

    max_retries = 4
    retry_count = 0
    retry_delay = 1

    while retry_count < max_retries:
        response = requests.get(search_url)
        if response.status_code == 200:
            break
        elif response.status_code == 500:
            retry_count += 1
            logger.warning(f"Server error (500) for city {city_string}. Retry {retry_count}/{max_retries} in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)
        else:
            break

    if response.status_code == 200:
        data = response.json()
        if items := data.get('data'):
            for item in items:
                """
                {
                    "location_id": "311293",
                    "name": "Tianjin",
                    "distance": "4.142198689704845",
                    "bearing": "south",
                    "address_obj": {
                        "street1": "",
                        "street2": "",
                        "state": "Tianjin Region",
                        "country": "China",
                        "postalcode": "",
                        "address_string": "Tianjin China"
                    }
                }
                """

                if str(item.get('name')).lower() == str(city['name']).lower() and item.get('address_obj', {}).get('country', '').lower() == city['country']['name'].lower():
                    logger.info(f"Found matching city: {item['name']} in {item['address_obj']['country']} with Geo ID: {item['location_id']}")
                    return item['location_id']

            logger.warning(f"No exact match found for city: {city_string}. Available items: {[item['name'] for item in items]}")
        else:
            logger.warning(f"No data found for city: {city_string}")
    else:
        logger.error(f"Error searching for city {city_string}: {response.status_code} - {response.text}")
        response.raise_for_status()
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TripAdvisor City Geo ID Manager')
    parser.add_argument('--check', action='store_true', help='Check existing geo IDs for correctness')
    args = parser.parse_args()

    if args.check:
        logger.info("Starting check mode - verifying existing geo IDs")
        check_all_cities_geo_ids()
    else:
        logger.info("Starting normal mode - finding geo IDs for cities without them")
        cities = get_non_geoid_cities()
        for city in cities:
            sleep(0.3)
            if city.get('region') is not None:
                if region := city['region']['name']:
                    city_string = f"{city['name']} {region} {city['country']['name']}"
                else:
                    city_string = f"{city['name']} {city['country']['name']}"
            else:
                city_string = f"{city['name']} {city['country']['name']}"

            try:
                logger.info(f"Processing city: {unidecode(city_string)} (GeoName ID: {city['geoname_id']})")
                geo_id = search_city_on_tripadvisor(city, unidecode(city_string))
                if geo_id is None:
                    logger.error(f"No Geo ID found for city: {city_string}")
                    update_last_scraped(city['geoname_id'])
                    continue
            except Exception as e:
                logger.error(f"Error searching for city {city_string}: {e}")
                update_last_scraped(city['geoname_id'])
                continue

            try:
                update_city_geo_id(city['geoname_id'], geo_id)
                update_last_scraped(city['geoname_id'])
            except Exception as e:
                logger.error(f"Error updating city {city['geoname_id']}: {e}")
                update_last_scraped(city['geoname_id'])
                continue

