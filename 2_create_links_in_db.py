import requests
import time
import argparse
import json
from typing import List, Dict, Optional


def fetch_single_city_by_tripadvisor_geo_id(tripadvisor_geo_id: int) -> Optional[Dict]:
    """
    Fetch a single city by its tripadvisor_geo_id.
    
    Args:
        tripadvisor_geo_id: The TripAdvisor geo ID to fetch
        
    Returns:
        City dict if found, None otherwise
    """
    print(f"Fetching city with tripadvisor_geo_id: {tripadvisor_geo_id}")
    
    # Search for city with this tripadvisor_geo_id
    url = f"http://127.0.0.1:8000/api/cities/search/?tripadvisor_geo_id={tripadvisor_geo_id}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            # Handle response structure
            items = []
            if isinstance(data, dict):
                if 'results' in data:
                    items = data.get('results', [])
                elif 'items' in data:
                    items = data.get('items', [])
            elif isinstance(data, list):
                items = data
            
            if items and len(items) > 0:
                city = items[0]  # Take the first match
                # Validate city has required fields
                if (city.get('tripadvisor_geo_id') and 
                    city.get('tripadvisor_restaurants_results') and
                    city.get('geoname_id')):
                    print(f"Found city: {city.get('name', 'Unknown')} with {city.get('tripadvisor_restaurants_results')} restaurants")
                    return city
                else:
                    print(f"City with tripadvisor_geo_id {tripadvisor_geo_id} is missing required fields")
                    return None
            else:
                print(f"City with tripadvisor_geo_id {tripadvisor_geo_id} not found")
                return None
        else:
            print(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching city: {e}")
        return None


def fetch_all_cities(country_code: Optional[str] = None, blacklisted_countries: Optional[List[str]] = None) -> List[Dict]:
    """
    Fetch all cities with restaurant data using Django Ninja pagination.
    Uses page and page_size parameters until no more results are returned.
    
    Args:
        country_code: Optional ISO 2-letter country code to filter cities (e.g., 'US', 'NL')
        blacklisted_countries: Optional list of ISO 2-letter country codes to exclude
    """
    all_cities = []
    page = 1
    page_size = 1000
    
    if country_code:
        print(f"Fetching cities with restaurant data for country: {country_code}")
    else:
        print("Fetching cities with restaurant data for all countries...")
    
    if blacklisted_countries:
        print(f"Excluding countries: {', '.join(blacklisted_countries)}")
    
    while True:
        url = (
            f"http://127.0.0.1:8000/api/cities/search/"
            f"?page={page}"
            f"&page_size={page_size}"
            f"&restaurants_is_null=false"
            f"&tripadvisor_geo_id_is_null=false"
        )
        
        # Add country filter if specified
        if country_code:
            url += f"&country={country_code}"
        
        print(f"Fetching page {page} (page_size: {page_size})...")
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                
                # Debug: print response structure for first page
                if page == 1:
                    print(f"Response type: {type(data)}")
                    if isinstance(data, dict):
                        print(f"Response keys: {list(data.keys())}")
                        if len(data) > 0:
                            # Show first few keys to understand structure
                            for key, value in list(data.items())[:3]:
                                print(f"  {key}: {type(value)}")
                
                # Handle different response structures
                items = []
                count = 0
                
                if isinstance(data, dict):
                    # Try common pagination field names
                    if 'items' in data:
                        items = data.get('items', [])
                        count = data.get('count', len(items))
                    elif 'results' in data:
                        items = data.get('results', [])
                        count = data.get('count', len(items))
                    elif 'data' in data:
                        items = data.get('data', [])
                        count = data.get('total', len(items))
                    else:
                        # Check if the dict contains city data directly
                        # Look for common city fields to identify if this is city data
                        if any(key in data for key in ['geoname_id', 'name', 'tripadvisor_geo_id']):
                            items = [data]  # Single city object
                            count = 1
                        else:
                            # Try to find a list value in the dict
                            for key, value in data.items():
                                if isinstance(value, list) and len(value) > 0:
                                    # Check if it looks like city data
                                    if isinstance(value[0], dict) and 'geoname_id' in value[0]:
                                        items = value
                                        count = data.get('count', len(items))
                                        break
                        
                elif isinstance(data, list):
                    items = data
                    count = len(data)
                
                if not items:
                    print(f"No cities found on page {page}")
                    break
                
                # Filter cities that have required data
                valid_cities = []
                for city in items:
                    if (city.get('tripadvisor_geo_id') and 
                        city.get('tripadvisor_restaurants_results') and
                        city.get('geoname_id')):
                        
                        # Apply blacklist filter
                        city_country = city.get('country_code')
                        if blacklisted_countries and city_country in blacklisted_countries:
                            continue  # Skip blacklisted countries
                        
                        valid_cities.append(city)
                
                if valid_cities:
                    all_cities.extend(valid_cities)
                    print(f"Found {len(valid_cities)} valid cities on page {page} "
                          f"(total so far: {len(all_cities)})")
                else:
                    print(f"No valid cities found on page {page}")
                
                # Check if we should continue to next page
                if len(items) < page_size:
                    print("Reached end of results (fewer items than page_size)")
                    break
                
                # If we have count info, check if we've reached the end
                if count > 0:
                    total_pages = (count + page_size - 1) // page_size
                    if page >= total_pages:
                        print(f"Reached last page ({page}/{total_pages})")
                        break
                
                page += 1
                
            else:
                print(f"API error: {response.status_code} - {response.text}")
                break
                
        except Exception as e:
            print(f"Error fetching cities: {e}")
            break
    
    print(f"Total cities fetched: {len(all_cities)}")
    return all_cities


def generate_restaurant_urls(city: Dict) -> List[str]:
    """
    Generate all TripAdvisor restaurant URLs for a city.
    Creates base URL and pagination URLs based on results count.
    """
    geo_id = city.get('tripadvisor_geo_id')
    results = city.get('tripadvisor_restaurants_results', 0)

    if not geo_id or results <= 0:
        return []

    # Calculate number of pages (30 results per page)
    pages = results // 30 + (1 if results % 30 > 0 else 0)

    base_url = (
        "https://www.tripadvisor.com/FindRestaurants"
        f"?geo={geo_id}"
        f"&establishmentTypes=10591,11776,12208,16548,16556,9900,9901,9909,21908"
        f"&minimumTravelerRating=TRAVELER_RATING_LOW"
        f"&broadened=false")

    urls = [f"{base_url}&offset=0"]  # First page (no offset)

    # Add paginated URLs
    if pages > 1:
        for i in range(1, pages):
            paginated_url = f"{base_url}&offset={i * 30}"
            urls.append(paginated_url)

    return urls


def add_restaurant_link_via_api(geoname_id: int, url: str, status: str = "pending") -> bool:
    """
    Add a restaurant link via the API with retry mechanism for network failures.
    
    Args:
        geoname_id: The geoname ID for the city
        url: The TripAdvisor restaurant list URL
        status: Status of the link (pending, completed, in_progress)
        
    Returns:
        True if successfully added, False if already exists or error
    """
    max_retries = 3
    retry_delay = 1.0
    
    payload = {
        "city_geoname_id": geoname_id,
        "link": url,
        "status": status
    }
    
    api_url = "http://127.0.0.1:8000/api/restaurant-links/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(api_url, json=payload, headers=headers)
            
            if response.status_code in [200, 201]:
                return True
            elif response.status_code == 409 or response.status_code == 400:
                # Conflict - URL already exists or validation error
                try:
                    error_detail = response.json()
                    if "already exists" in str(error_detail).lower() or "duplicate" in str(error_detail).lower():
                        return False  # Already exists, not an error
                except json.JSONDecodeError:
                    pass
                return False
            elif response.status_code >= 500 and attempt < max_retries - 1:
                # Server error, retry
                print(f"Server error ({response.status_code}), retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                # Other error
                try:
                    error_detail = response.json()
                    print(f"API error {response.status_code}: {error_detail}")
                except json.JSONDecodeError:
                    print(f"API error {response.status_code}: {response.text[:200]}")
                return False
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Network error, retrying in {retry_delay}s: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                print(f"Network error after {max_retries} attempts: {e}")
                return False
        except Exception as e:
            print(f"Error adding restaurant link: {e}")
            return False
    
    return False


def remove_existing_restaurant_links(geoname_id: int) -> int:
    """
    Remove all existing restaurant links for a city.
    This is needed when a city's tripadvisor_geo_id changes or when refreshing data.
    
    Args:
        geoname_id: The geoname ID of the city
        
    Returns:
        Number of links deleted
    """
    deleted_count = 0
    page = 1
    page_size = 100
    
    print(f"Removing existing restaurant links for geoname_id {geoname_id}...")
    
    while True:
        # Fetch links for this geoname_id
        url = f"http://127.0.0.1:8000/api/restaurant-links/search/?city_geoname_id={geoname_id}&page={page}&page_size={page_size}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                
                # Handle response structure
                items = []
                if isinstance(data, dict):
                    if 'results' in data:
                        items = data.get('results', [])
                    elif 'items' in data:
                        items = data.get('items', [])
                elif isinstance(data, list):
                    items = data
                
                if not items:
                    break
                
                # Delete each link
                for link in items:
                    link_id = link.get('id')
                    if link_id:
                        delete_url = f"http://127.0.0.1:8000/api/restaurant-links/{link_id}/"
                        try:
                            delete_response = requests.delete(delete_url)
                            if delete_response.status_code in [200, 204]:
                                deleted_count += 1
                            else:
                                print(f"Failed to delete link {link_id}: {delete_response.status_code}")
                        except Exception as e:
                            print(f"Error deleting link {link_id}: {e}")
                
                # Check if we should continue to next page
                if len(items) < page_size:
                    break
                    
                page += 1
                
            else:
                print(f"Error fetching restaurant links: {response.status_code}")
                break
                
        except Exception as e:
            print(f"Error removing restaurant links: {e}")
            break
    
    if deleted_count > 0:
        print(f"Removed {deleted_count} existing restaurant links for geoname_id {geoname_id}")
    
    return deleted_count


def add_urls_to_database(city: Dict, urls: List[str]) -> int:
    """
    Add URLs to database for a given city.
    Returns count of successfully added URLs.
    """
    if not urls:
        return 0
    
    geoname_id = city.get('geoname_id')
    city_name = city.get('name', 'Unknown')
    added_count = 0
    
    for url in urls:
        if add_restaurant_link_via_api(geoname_id, url, "pending"):
            added_count += 1
    
    if added_count > 0:
        print(f"Added {added_count}/{len(urls)} URLs for {city_name}")
    else:
        print(f"No new URLs added for {city_name} "
              f"(all {len(urls)} already exist)")
    
    return added_count


def main():
    """Main function to create restaurant links via API."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Create TripAdvisor restaurant links via API'
    )
    parser.add_argument(
        '--country', '-c',
        type=str,
        default=None,
        help='ISO 2-letter country code to filter cities (e.g., US, NL, FR). If not specified, processes all countries.'
    )
    parser.add_argument(
        '--geo-id', '-g',
        type=int,
        default=None,
        help='Specific TripAdvisor geo ID to process (overrides country option)'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Limit the number of cities to process'
    )
    parser.add_argument(
        '--blacklist', '-b',
        type=str,
        default=None,
        help='Comma-separated list of ISO 2-letter country codes to exclude (e.g., "US,CN,RU")'
    )
    parser.add_argument(
        '--clean', 
        action='store_true',
        default=True,
        help='Clean old restaurant links before adding new ones (default: True)'
    )
    parser.add_argument(
        '--no-clean',
        action='store_false',
        dest='clean',
        help='Do not clean old restaurant links before adding new ones'
    )
    
    args = parser.parse_args()
    country = args.country.upper() if args.country and not args.geo_id else None
    
    # Parse blacklist
    blacklisted_countries = None
    if args.blacklist and not args.geo_id:
        blacklisted_countries = [c.strip().upper() for c in args.blacklist.split(',')]
    
    print("=" * 60)
    print("CREATING RESTAURANT LINKS VIA API")
    print("=" * 60)
    
    if args.geo_id:
        print(f"Target: Single TripAdvisor geo_id {args.geo_id}")
    elif country:
        print(f"Target country: {country}")
    else:
        print("Target: All countries")
    
    if blacklisted_countries:
        print(f"Blacklisted countries: {', '.join(blacklisted_countries)}")
    
    if args.limit and not args.geo_id:
        print(f"City limit: {args.limit}")
    
    if args.clean:
        print("Clean mode: ENABLED (will remove old links before adding new ones)")
    else:
        print("Clean mode: DISABLED (old links will be kept)")
    
    # No database initialization needed - using API
    
    # Fetch cities
    if args.geo_id:
        # Fetch single city by tripadvisor_geo_id
        city = fetch_single_city_by_tripadvisor_geo_id(args.geo_id)
        cities = [city] if city else []
    else:
        # Fetch all cities based on filters
        cities = fetch_all_cities(country, blacklisted_countries)
    
    if not cities:
        print("No cities found to process.")
        return
    
    # Apply limit if specified (not for single geo_id)
    if args.limit and not args.geo_id and len(cities) > args.limit:
        cities = cities[:args.limit]
        print(f"Limited to first {args.limit} cities")
    
    print(f"\nProcessing {len(cities)} cities...")
    
    # Process each city
    total_urls_generated = 0
    total_urls_added = 0
    cities_processed = 0
    
    for i, city in enumerate(cities, 1):
        city_name = city.get('name', 'Unknown')
        results_count = city.get('tripadvisor_restaurants_results', 0)
        geoname_id = city.get('geoname_id')
        
        print(f"\n[{i}/{len(cities)}] Processing {city_name} "
              f"({results_count} results)...")
        
        # First remove any existing links for this city (if cleaning is enabled)
        if geoname_id and args.clean:
            removed_count = remove_existing_restaurant_links(geoname_id)
            if removed_count > 0:
                print(f"Cleaned up {removed_count} old links before adding new ones")
        
        # Generate URLs
        urls = generate_restaurant_urls(city)
        total_urls_generated += len(urls)
        
        if urls:
            # Add to database
            added_count = add_urls_to_database(city, urls)
            total_urls_added += added_count
            cities_processed += 1
        else:
            print(f"No URLs generated for {city_name}")
        
        # Progress update every 50 cities
        if i % 50 == 0:
            print(f"\nProgress: {i}/{len(cities)} cities processed "
                  f"({i*100//len(cities)}%)")
            print(f"URLs generated: {total_urls_generated}, "
                  f"URLs added: {total_urls_added}")
    
    # Final summary
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total cities fetched from API: {len(cities)}")
    print(f"Cities with URLs generated: {cities_processed}")
    print(f"Total URLs generated: {total_urls_generated}")
    print(f"Total URLs added to database: {total_urls_added}")
    print(f"URLs already existed: {total_urls_generated - total_urls_added}")
    
    if cities_processed > 0:
        avg_urls_per_city = total_urls_generated / cities_processed
        print(f"Average URLs per city: {avg_urls_per_city:.1f}")
    
    print("\nAPI link creation completed successfully!")


if __name__ == "__main__":
    main()