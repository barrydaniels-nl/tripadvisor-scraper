from camoufox.sync_api import Camoufox
import requests
import json
from typing import List, Dict, Any


def get_big_cities():
    response = requests.get(
        "http://127.0.0.1:8000/api/cities/search/?min_restaurants=10000"
    )

    if response.status_code == 200:
        cities = response.json()['results'][11]
        return cities
    else:
        print(f"Error fetching cities: {response.status_code}")
        response.raise_for_status()


def extract_jsonld_restaurants(page) -> List[Dict[str, Any]]:
    """
    Extract restaurant data from JSON-LD structured data on the page.
    Returns a list of restaurant dictionaries with parsed information.
    """
    restaurants = []

    try:
        # Execute JavaScript to extract the JSON-LD content
        jsonld_content = page.evaluate("""
            () => {
                const div = document.querySelector(
                    'div[data-automation="restaurant-list-jsonld"]'
                );
                if (div) {
                    const script = div.querySelector(
                        'script[type="application/ld+json"]'
                    );
                    if (script) {
                        return script.textContent;
                    }
                }
                return null;
            }
        """)

        if jsonld_content:
            # Parse the JSON-LD content
            jsonld_data = json.loads(jsonld_content)

            # Extract restaurant items from the ItemList
            if 'itemListElement' in jsonld_data:
                for item in jsonld_data['itemListElement']:
                    restaurant_info = {}

                    # Get basic info
                    restaurant_info['position'] = item.get('position')

                    # Some items might not have 'item' field
                    if 'item' in item:
                        rest_item = item['item']
                        restaurant_info['name'] = rest_item.get('name', '')
                        restaurant_info['description'] = rest_item.get(
                            'description', ''
                        )
                        restaurant_info['url'] = rest_item.get('url', '')
                        restaurant_info['priceRange'] = rest_item.get(
                            'priceRange', ''
                        )
                        restaurant_info['telephone'] = rest_item.get(
                            'telephone', ''
                        )
                        restaurant_info['image'] = rest_item.get('image', [])

                        # Extract rating information
                        if 'aggregateRating' in rest_item:
                            rating = rest_item['aggregateRating']
                            restaurant_info['rating'] = {
                                'value': rating.get('ratingValue'),
                                'reviewCount': rating.get('reviewCount')
                            }
                        else:
                            restaurant_info['rating'] = None

                        # Extract address information
                        if 'address' in rest_item:
                            addr = rest_item['address']
                            restaurant_info['address'] = {
                                'country': addr.get('addressCountry', ''),
                                'locality': addr.get('addressLocality', ''),
                                'region': addr.get('addressRegion', ''),
                                'postalCode': addr.get('postalCode', ''),
                                'streetAddress': addr.get('streetAddress', '')
                            }
                        else:
                            restaurant_info['address'] = None
                    else:
                        # Handle items with only name and URL
                        restaurant_info['name'] = item.get('name', '')
                        restaurant_info['url'] = item.get('url', '')
                        restaurant_info['incomplete'] = True

                    restaurants.append(restaurant_info)

            print(f"  Extracted {len(restaurants)} restaurants from JSON-LD")

    except Exception as e:
        print(f"  Error extracting JSON-LD data: {e}")

    return restaurants


def get_restaurants():
    city = get_big_cities()
    
    print(f"\n{'='*60}")
    print(f"Processing: {city['name']}, {city['country']}")
    print(f"{'='*60}\n")

    if 'tripadvisor_geo_id' not in city or not city['tripadvisor_geo_id']:
        print(f"Skipping {city['name']} due to missing tripadvisor_geo_id")
        quit()

    city_url = f"https://www.tripadvisor.com/FindRestaurants?geo={city['tripadvisor_geo_id']}&offset=9660&establishmentTypes=10591%2C11776%2C12208%2C16548%2C16556%2C9900%2C9901%2C9909&minimumTravelerRating=TRAVELER_RATING_LOW&broadened=false"

    # List to store all GraphQL responses
    #graphql_responses: List[Dict[str, Any]] = []

    # List to store all restaurant data from JSON-LD
    all_restaurants: List[Dict[str, Any]] = []

    with Camoufox(
        proxy={
            'server': 'https://premium-residential.evomi-proxy.com:1001',
            'username': 'proxies6',
            'password': 'kmcohO5P6DD0CGaVrfNH'
        }
    ) as browser:
        page = browser.new_page()

        # Set up response interceptor for GraphQL endpoints
        #def handle_response(response):
            # Check if this is a GraphQL endpoint
            # url_lower = response.url.lower()
            # if 'graphql' in url_lower or '/data/graphql' in url_lower:
            #     try:
            #         # Try to get JSON response
            #         response_data = response.json()

            #         # Store the response data
            #         graphql_responses.append({
            #             'url': response.url,
            #             'status': response.status,
            #             'data': response_data,
            #             'timestamp': len(graphql_responses)
            #         })

            #         print(
            #             f"  Captured GraphQL response "
            #             f"#{len(graphql_responses)}"
            #         )
            #         print(f"    URL: {response.url[:80]}...")
            #         print(f"    Status: {response.status}")

            #         # Print a sample of the data structure
            #         if response_data:
            #             if isinstance(response_data, dict):
            #                 data_keys = list(response_data.keys())
            #             else:
            #                 data_keys = type(response_data).__name__
            #             print(f"    Data keys: {data_keys}")

            #     except Exception as e:
            #         print(f"  Error parsing GraphQL response: {e}")

        # Attach the response handler
        #page.on('response', handle_response)

        # Navigate to the page
        print(f"Navigating to: {city_url}")
        page.goto(city_url, wait_until='networkidle', timeout=60000)

        # Wait a bit for initial content to load
        page.wait_for_timeout(5000)

        # Pagination: Keep clicking "Next page" until no more pages
        page_num = 1
        while True:
            print(f"\n--- Processing page {page_num} ---")

            # Scroll to load content on current page
            print("Scrolling to load more content...")
            for _ in range(2):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                page.wait_for_timeout(1500)

            # Extract JSON-LD restaurant data from current page
            page_restaurants = extract_jsonld_restaurants(page)
            if page_restaurants:
                # Add page number to each restaurant
                for restaurant in page_restaurants:
                    restaurant['page'] = page_num
                all_restaurants.extend(page_restaurants)
                print(
                    f"  Total restaurants collected: "
                    f"{len(all_restaurants)}"
                )

            # Try to find and click the "Next page" button
            try:
                next_button = page.locator('[aria-label="Next page"]')

                # Check if button exists and is enabled
                if next_button.count() > 0:
                    # Check if button is not disabled
                    is_disabled = next_button.get_attribute('disabled')
                    aria_disabled = next_button.get_attribute(
                        'aria-disabled'
                    )

                    if is_disabled == 'true' or aria_disabled == 'true':
                        print("Next page button is disabled. No more pages.")
                        break

                    print("Clicking 'Next page' button...")
                    next_button.click()

                    # Wait for new content to load
                    page.wait_for_timeout(3000)

                    # Wait for network to be idle
                    try:
                        page.wait_for_load_state(
                            'networkidle', timeout=10000
                        )
                    except Exception:
                        # Continue even if networkidle times out
                        pass

                    page_num += 1
                else:
                    print("No 'Next page' button found. Reached last page.")
                    break

            except Exception as e:
                print(f"Error or no more pages: {e}")
                break

            # Safety limit to prevent infinite loops
            if page_num > 1000:
                print("Reached maximum page limit (100). Stopping.")
                break

        print(f"\n{'='*40}")
        print(f"Total pages processed: {page_num}")
        #print(f"Total GraphQL responses captured: {len(graphql_responses)}")
        print(f"Total restaurants extracted: {len(all_restaurants)}")
        print(f"{'='*40}\n")

        # Print summary of captured GraphQL data
        # if graphql_responses:
        #     print("\n--- GraphQL Responses Summary ---")
        #     for idx, resp in enumerate(graphql_responses[:5], 1):
        #         print(f"Response #{idx}:")
        #         print(f"  URL: {resp['url'][:80]}...")
        #         if isinstance(resp['data'], dict):
        #             if 'data' in resp['data']:
        #                 # Common GraphQL structure
        #                 ops = resp['data']['data'].keys() if resp['data']['data'] else []
        #                 print(f"  GraphQL operation keys: {list(ops) if ops else 'None'}")
        #             else:
        #                 keys = list(resp['data'].keys())[:5]
        #                 print(f"  Response keys: {keys}...")
        #     if len(graphql_responses) > 5:
        #         print(f"... and {len(graphql_responses) - 5} more responses")

        # Print restaurant data summary
        if all_restaurants:
            print("\n--- Restaurant Data Summary ---")
            print(f"Total restaurants extracted: {len(all_restaurants)}")

            # Count restaurants with complete vs incomplete data
            complete = sum(
                1 for r in all_restaurants
                if not r.get('incomplete', False)
            )
            incomplete = len(all_restaurants) - complete
            print(f"  Complete data: {complete}")
            print(f"  Incomplete data: {incomplete}")

        # Save all data to files
        city_name_safe = city['name'].replace(' ', '_').replace('/', '_')

        # Save GraphQL responses
        # if graphql_responses:
        #     graphql_filename = f"graphql_responses_{city_name_safe}.json"
        #     with open(graphql_filename, 'w') as f:
        #         json.dump(graphql_responses, f, indent=2)
        #     print(f"\nSaved GraphQL responses to: {graphql_filename}")

        # Save restaurant data
        if all_restaurants:
            restaurants_filename = f"restaurants_{city_name_safe}.json"
            with open(restaurants_filename, 'w') as f:
                json.dump(all_restaurants, f, indent=2)
            print(f"Saved restaurant data to: {restaurants_filename}")

        # Save combined data
        # combined_data = {
        #     'city': {
        #         'name': city['name'],
        #         'country': city['country'],
        #         'tripadvisor_geo_id': city['tripadvisor_geo_id']
        #     },
        #     'statistics': {
        #         'total_pages': page_num,
        #         'total_restaurants': len(all_restaurants),
        #         'total_graphql_responses': len(graphql_responses)
        #     },
        #     'restaurants': all_restaurants,
        #     'graphql_responses': graphql_responses
        # }
        # combined_filename = f"combined_data_{city_name_safe}.json"
        # with open(combined_filename, 'w') as f:
        #     json.dump(combined_data, f, indent=2)
        # print(f"Saved combined data to: {combined_filename}")


def main():
    get_restaurants()
    
if __name__ == "__main__":
    main()