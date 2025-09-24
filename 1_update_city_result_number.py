from spider_cloud import SpiderAPI
import requests
import dotenv
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

dotenv.load_dotenv()

client = SpiderAPI()


def get_empty_results_city():
    """Fetches cities with no TripAdvisor restaurant URL or results."""
    url = (
        "http://127.0.0.1:8000/api/cities/search/"
        "?tripadvisor_geo_id_is_null=false&page_size=10000"
    )

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data['results']
        else:
            raise ValueError("No Cities with null results found")


def parse_results_number(city_data: dict) -> tuple[Optional[int], bool]:
    """
    Filters out the number of results from the city data using data-automation='resultsTotal'.
    
    Returns:
        tuple[Optional[int], bool]: (results_count, parsing_successful)
        - results_count: The number of results found, or None if not found/error
        - parsing_successful: True if parsing was successful (even if 0 results), False if error occurred
    """
    from bs4 import BeautifulSoup
    
    try:
        content = city_data[0]['content']
        # Check if content is already a string or needs decoding
        if isinstance(content, str):
            decoded = content
        else:
            # If it's a list of character codes, decode it
            decoded = ''.join(chr(c) for c in content)
        soup = BeautifulSoup(decoded, 'html.parser')
        
        # Find element with data-automation="resultsTotal"
        element = soup.find(attrs={"data-automation": "resultsTotal"})

        if element:
            # Extract text and parse number
            text = element.get_text(strip=True)
            
            # Use regex to extract numbers from text like "1,234 results" or "234 results"
            match = re.search(r'([\d,]+)', text)
            if match:
                # Remove commas and convert to int
                number_str = match.group(1).replace(',', '')
                result = int(number_str)
                return result, True
            else:
                # Check if the text indicates 0 results explicitly
                if 'no results' in text.lower() or '0 results' in text.lower():
                    print(f"Found explicit zero results indication: '{text}'")
                    return 0, True
                print(f"Could not parse number from text: '{text}'")
                return None, False
        else:
            print("Element with data-automation='resultsTotal' not found")
            # Fallback: try to find any element containing "results"
            elements = soup.find_all(string=re.compile(r'\d+.*results', re.I))
            if elements:
                for text in elements:
                    match = re.search(r'([\d,]+)', text)
                    if match:
                        number_str = match.group(1).replace(',', '')
                        result = int(number_str)
                        print(f"Found results using fallback method: {result}")
                        return result, True
            
            # Check for explicit "no results" messages
            no_results_elements = soup.find_all(string=re.compile(r'no.*results|0.*results', re.I))
            if no_results_elements:
                print("Found 'no results' indication")
                return 0, True
                
            return None, False
            
    except Exception as e:
        print(f"Error parsing results number: {e}")
        return None, False


def update_city_results_number(geoname_id: int, results: int) -> bool:
    """Updates city restaurant results via API."""
    url = (
        f"http://127.0.0.1:8000/api/cities/{geoname_id}/"
        f"update-tripadvisor-results/"
    )

    payload = {
        "tripadvisor_restaurants_results": results,
    }

    response = requests.patch(url, json=payload)

    if response.status_code == 200:
        return True
    else:
        print(
            f"Failed to update city results: "
            f"{response.status_code} - {response.text}"
        )
        return False


def process_city(city: Dict) -> Dict:
    """
    Process a single city: fetch data, parse results, update via API.
    Returns a dictionary with the processing results.
    """
    result = {
        'city_name': city.get('name'),
        'geoname_id': city.get('geoname_id'),
        'success': False,
        'error': None,
        'results_count': None
    }

    try:
        tripadvisor_geo_id = city.get("tripadvisor_geo_id")
        geoname_id = city.get("geoname_id")

        # Validate city data
        if (not tripadvisor_geo_id or not geoname_id):
            result['error'] = "Missing required data"
            print(f"Skipping city {city['name']} due to missing data.")
            return result

        print(f"[Thread] Analyzing city {city['name']}...")

        # Build TripAdvisor URL
        tripadvisor_restaurants_url = (
            "https://www.tripadvisor.com/FindRestaurants"
            f"?geo={tripadvisor_geo_id}"
            f"&establishmentTypes=10591,11776,12208,16548,16556,9900,9901,9909,21908"
            f"&minimumTravelerRating=TRAVELER_RATING_LOW"
            f"&broadened=false"
            f"&offset=0"
        )

        # Fetch data from Spider API
        tripadvisor_city_data = client.request_spider_api(
            "results", tripadvisor_restaurants_url
        )

        if tripadvisor_city_data[0]['status'] != 200 or tripadvisor_city_data[0]['error'] is not None:
            error_msg = tripadvisor_city_data.get('error', 'Unknown error')
            result['error'] = f"API fetch failed: {error_msg}"
            print(f"Failed to fetch data for {city['name']}: {error_msg}")
            return result

        # Parse results
        results, parsing_successful = parse_results_number(tripadvisor_city_data)

        if parsing_successful:
            # Parsing was successful, results could be 0 or a positive number
            result['results_count'] = results
            print(
                f"Found {results} results for {city['name']} "
                f"(geoname_id: {geoname_id})"
            )

            # Update city results via API
            if update_city_results_number(geoname_id, results):
                print(
                    f"Successfully updated {city['name']} "
                    f"with {results} results."
                )
                result['success'] = True
            else:
                result['error'] = "Failed to update via API"
                print(f"Failed to update {city['name']} results via API.")
        else:
            # Parsing failed, don't update the database
            result['error'] = "Failed to parse results from scraped data"
            print(
                f"Failed to parse results for {city.get('name')} "
                f"(geoname_id: {geoname_id}) - not updating database"
            )

    except Exception as e:
        result['error'] = str(e)
        print(f"Error processing city {city.get('name', 'Unknown')}: {e}")

    return result


def get_city_by_tripadvisor_geo_id(tripadvisor_geo_id: str) -> list:
    """Fetches a single city by its tripadvisor_geo_id."""
    url = (
        "http://127.0.0.1:8000/api/cities/search/"
        f"?tripadvisor_geo_id={tripadvisor_geo_id}"
        "&page_size=1"
    )
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data and 'results' in data and len(data['results']) > 0:
            print(f"Found city: {data['results'][0]['name']} (geoname_id: {data['results'][0]['geoname_id']})")
            return data['results']
        else:
            print(f"No city found with tripadvisor_geo_id: {tripadvisor_geo_id}")
            return []
    else:
        print(f"API error: {response.status_code}")
        return []


def get_zero_results_cities_with_url(country_code: Optional[str] = None) -> list:
    """Fetches cities with tripadvisor_restaurants_results=0 but valid tripadvisor_restaurants_url."""
    url = (
        "http://127.0.0.1:8000/api/cities/search/"
        "?max_restaurants=0"
        "&tripadvisor_restaurants_url_is_null=false"
        "&tripadvisor_geo_id_is_null=false"
        "&page_size=5000"
    )
    
    if country_code:
        url += f"&country={country_code}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data and 'results' in data:
            return data['results']
        else:
            print("No cities with zero results but valid URL found")
            return []
    else:
        print(f"API error: {response.status_code}")
        return []


def main():
    """Main function to update city restaurant results."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Update city restaurant results from TripAdvisor'
    )
    parser.add_argument(
        '--country', 
        type=str, 
        help='ISO 2-letter country code to filter cities (e.g., US, FR, IT)'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=None,
        help='Limit the number of cities to process'
    )
    parser.add_argument(
        '--zero-results-only',
        action='store_true',
        help='Only process cities with restaurants_results=0 but valid URL'
    )
    parser.add_argument(
        '--geo-id',
        type=str,
        help='Process only one specific tripadvisor_geo_id'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.geo_id and (args.country or args.zero_results_only):
        print("Error: --geo-id cannot be combined with --country or --zero-results-only")
        return
    
    # Choose which cities to fetch based on the mode
    if args.geo_id:
        print(f"Mode: Processing single city with tripadvisor_geo_id: {args.geo_id}")
        cities = get_city_by_tripadvisor_geo_id(args.geo_id)
        mode_desc = f"city with geo_id {args.geo_id}"
    elif args.zero_results_only:
        print("Mode: Processing cities with zero results but valid URLs...")
        cities = get_zero_results_cities_with_url(args.country)
        mode_desc = f"cities with zero results{' in ' + args.country if args.country else ''}"
    else:
        print("Mode: Processing cities with null restaurant data...")
        cities = get_empty_results_city()
        mode_desc = "cities with null restaurant data"
    
    if not cities:
        print(f"No {mode_desc} to process.")
        return
    
    # Apply country filter for the original mode if needed (skip if using geo_id)
    if not args.geo_id and not args.zero_results_only and args.country:
        cities = [city for city in cities if city.get('country', {}).get('code') == args.country.upper()]
        if not cities:
            print(f"No cities found for country: {args.country}")
            return
    
    # Apply limit if specified (skip if using geo_id since it's always 1 city)
    if not args.geo_id and args.limit and len(cities) > args.limit:
        cities = cities[:args.limit]
        print(f"Limited to first {args.limit} cities")
    
    print(f"Found {len(cities)} {mode_desc} to process.")

    print(f"Starting concurrent processing of {len(cities)} cities...")
    print("Processing with up to 10 concurrent threads...")

    # Process cities concurrently
    results = []
    with ThreadPoolExecutor(max_workers=25) as executor:
        # Submit all tasks
        future_to_city = {
            executor.submit(process_city, city): city 
            for city in cities
        }

        # Process completed tasks
        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                result = future.result()
                results.append(result)
                
                # Print progress
                completed = len(results)
                total = len(cities)
                print(
                    f"Progress: {completed}/{total} cities processed "
                    f"({completed*100//total}%)"
                )
                
            except Exception as exc:
                print(f"City {city.get('name', 'Unknown')} generated "
                      f"an exception: {exc}")
                results.append({
                    'city_name': city.get('name'),
                    'geoname_id': city.get('geoname_id'),
                    'success': False,
                    'error': str(exc)
                })

    # Print summary
    print("\n" + "="*50)
    print("PROCESSING SUMMARY")
    print("="*50)
    
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"Total cities processed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    if failed > 0:
        print("\nFailed cities:")
        for result in results:
            if not result['success']:
                print(f"  - {result['city_name']}: {result['error']}")


if __name__ == "__main__":
    main()