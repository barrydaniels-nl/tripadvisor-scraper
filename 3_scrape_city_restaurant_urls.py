import requests
import dotenv
import json
import argparse
import time
from datetime import datetime
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import get_city_restaurant_urls_with_status, remove_city_restaurant_url
from spider_cloud import SpiderAPI

client = SpiderAPI()

dotenv.load_dotenv()

DATABASE_FILE = "city_restaurant_links.db"


def get_geoname_id_from_tripadvisor_geo_id(tripadvisor_geo_id: int) -> int:
    """
    Get geoname_id for a given tripadvisor_geo_id.
    
    Args:
        tripadvisor_geo_id: The TripAdvisor geo ID to look up
        
    Returns:
        The corresponding geoname_id if found, None otherwise
    """
    print(f"Looking up geoname_id for tripadvisor_geo_id: {tripadvisor_geo_id}")
    
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
                geoname_id = city.get('geoname_id')
                city_name = city.get('name', 'Unknown')
                restaurants_count = city.get('tripadvisor_restaurants_results', 0)
                
                if geoname_id:
                    print(f"Found city: {city_name} (geoname_id: {geoname_id}) with {restaurants_count} restaurants")
                    return geoname_id
                else:
                    print(f"City found but missing geoname_id")
                    return None
            else:
                print(f"No city found with tripadvisor_geo_id {tripadvisor_geo_id}")
                return None
        else:
            print(f"API error: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error looking up city: {e}")
        return None


def get_tripadvisor_geo_ids_by_country(country_code: str) -> List[str]:
    """
    Get all TripAdvisor geo IDs for cities in a specific country.
    
    Args:
        country_code: ISO 2-letter country code (e.g., 'US', 'FR', 'DE')
        
    Returns:
        List of TripAdvisor geo IDs as strings
    """
    all_geo_ids = []
    page = 1
    page_size = 100
    
    print(f"Fetching TripAdvisor geo IDs for country code: {country_code}")
    
    while True:
        url = (
            f"http://127.0.0.1:8000/api/cities/search/"
            f"?page={page}"
            f"&page_size={page_size}"
            f"&country={country_code}"
            f"&tripadvisor_geo_id_is_null=false"
        )
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                
                # Handle different response structures
                items = []
                if isinstance(data, dict):
                    if 'items' in data:
                        items = data.get('items', [])
                    elif 'results' in data:
                        items = data.get('results', [])
                    elif 'data' in data:
                        items = data.get('data', [])
                elif isinstance(data, list):
                    items = data
                
                if not items:
                    break
                
                # Extract TripAdvisor geo IDs
                for city in items:
                    tripadvisor_geo_id = city.get('tripadvisor_geo_id')
                    if tripadvisor_geo_id:
                        all_geo_ids.append(str(tripadvisor_geo_id))
                
                # Check if we should continue to next page
                if len(items) < page_size:
                    break
                
                page += 1
                
            else:
                print(f"API error: {response.status_code}")
                break
                
        except Exception as e:
            print(f"Error fetching TripAdvisor geo IDs: {e}")
            break
    
    print(f"Total TripAdvisor geo IDs found for {country_code}: {len(all_geo_ids)}")
    return all_geo_ids


def get_geoname_ids_by_country(country_code: str) -> List[int]:
    """
    Get all geoname_ids for cities in a specific country that have TripAdvisor restaurant data.
    
    Args:
        country_code: ISO 2-letter country code (e.g., 'US', 'FR', 'DE')
        
    Returns:
        List of geoname_ids for cities with tripadvisor_restaurants_results > 0 and tripadvisor_geo_id
    """
    all_geoname_ids = []
    page = 1
    page_size = 100
    
    print(f"Fetching cities for country code: {country_code}")
    
    while True:
        url = (
            f"http://127.0.0.1:8000/api/cities/search/"
            f"?page={page}"
            f"&page_size={page_size}"
            f"&country={country_code}"
            f"&tripadvisor_geo_id_is_null=false"
        )
        
        print(f"Fetching page {page} for {country_code}...")
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                
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
                    print(f"No more cities found on page {page}")
                    break
                
                # Filter cities that have required data
                valid_geoname_ids = []
                for city in items:
                    tripadvisor_geo_id = city.get('tripadvisor_geo_id')
                    tripadvisor_restaurants_results = city.get('tripadvisor_restaurants_results', 0)
                    geoname_id = city.get('geoname_id')
                    
                    if (tripadvisor_geo_id and 
                        tripadvisor_restaurants_results > 0 and 
                        geoname_id):
                        valid_geoname_ids.append(geoname_id)
                
                if valid_geoname_ids:
                    all_geoname_ids.extend(valid_geoname_ids)
                    print(f"Found {len(valid_geoname_ids)} valid cities on page {page} "
                          f"(total so far: {len(all_geoname_ids)})")
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
    
    print(f"Total geoname_ids found for {country_code}: {len(all_geoname_ids)}")
    return all_geoname_ids


def add_restaurant_basic_info(data: dict) -> dict:
    """Adds basic restaurant information to the API.

    Args:
        data (dict): Restaurant data from TripAdvisor JSON structure

    Returns:
        dict: API response or error information
    """
    if not data:
        return {"error": "No data provided"}
    
    try:
        # Safely extract rating information
        aggregate_rating = data.get("aggregateRating", {})
        rating_value = ""
        review_count = ""
        
        if isinstance(aggregate_rating, dict):
            rating_value = aggregate_rating.get("ratingValue", "")
            review_count = aggregate_rating.get("reviewCount", "")
        
        # Safely extract address information
        address = data.get("address", {})
        if isinstance(address, dict):
            street_address = address.get("streetAddress", "")
            postal_code = address.get("postalCode", "")
            city = address.get("addressLocality", "")
            country = address.get("addressCountry", "")
        else:
            street_address = postal_code = city = country = ""
        
        # Safely extract image
        image_list = data.get("image", [])
        first_image = ""
        if isinstance(image_list, list) and len(image_list) > 0:
            first_image = image_list[0]
        
        payload = {
            "name": data.get("name", ""),
            "tripadvisor_detail_page": data.get("url", ""),
            "address_string": street_address,
            "postal_code": postal_code,
            "city": city,
            "country": country,
            "rating": str(rating_value),
            "num_reviews": str(review_count),
            "price_range": data.get("priceRange", ""),
            "phone": data.get("telephone", ""),
            "image_urls": [first_image] if first_image else [],
            "city_geoname_id": data.get("city_geoname_id", ""),
        }

        url = "http://127.0.0.1:8000/api/restaurants/"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Fix: Don't double-encode JSON
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code in [200, 201]:
            print(f"Restaurant {data.get('name', '')} added successfully.")
            return {"success": True, "response": response.json()}
        else:
            error_detail = ""
            try:
                error_detail = response.json()
            except json.JSONDecodeError:
                error_detail = response.text
            
            print(f"Failed to add restaurant {data.get('name', '')}. "
                  f"Status: {response.status_code}")
            print(f"Error details: {error_detail}")
            return {"error": f"API error {response.status_code}", 
                   "details": error_detail}

    except Exception as e:
        print(f"Error processing restaurant {data.get('name', 'Unknown')}: {e}")
        return {"error": str(e)}


def detect_captcha(response_data: dict) -> bool:
    """
    Detect if response contains a CAPTCHA challenge.
    
    Args:
        response_data: Response data from Spider API
        
    Returns:
        True if CAPTCHA detected, False otherwise
    """
    if not response_data:
        return False
    
    # Check for common CAPTCHA indicators in the response
    captcha_indicators = [
        'captcha', 'recaptcha', 'challenge', 'verify', 'robot',
        'human verification', 'security check', 'access denied'
    ]
    
    # Check HTML content if available
    html_content = response_data.get('content', '')
    if html_content:
        html_lower = html_content.lower()
        for indicator in captcha_indicators:
            if indicator in html_lower:
                return True
    
    # Check page title
    title = response_data.get('title', '')
    if title:
        title_lower = title.lower()
        for indicator in captcha_indicators:
            if indicator in title_lower:
                return True
    
    return False


def validate_response_size(response_data: dict, min_size_kb: int = 10) -> bool:
    """
    Validate if response size meets minimum requirements.
    
    Args:
        response_data: Response data from Spider API
        min_size_kb: Minimum expected size in kilobytes
        
    Returns:
        True if response size is adequate, False otherwise
    """
    if not response_data:
        return False
    
    # Get content length from response
    content = response_data.get('content', '')
    content_size = len(content) if content else 0
    
    # Check if JSON data exists and has reasonable size
    json_data = response_data.get('json_data', {})
    json_size = len(str(json_data)) if json_data else 0
    
    total_size = content_size + json_size
    min_size_bytes = min_size_kb * 1024
    
    return total_size >= min_size_bytes


def process_single_url(url_tuple: Tuple[str, str], geoname_id: int, expected_geo_ids: List[str] = None) -> Dict:
    """
    Process a single URL and extract restaurant data.
    
    Args:
        url_tuple: Tuple of (url, status)
        geoname_id: The geoname ID for the city
        expected_geo_ids: List of expected TripAdvisor geo IDs for validation (optional)
        
    Returns:
        Dictionary with processing results
    """
    url, status = url_tuple
    result = {
        'url': url,
        'geoname_id': geoname_id,
        'successful_restaurants': 0,
        'failed_restaurants': 0,
        'restaurants_found': False,
        'should_remove_url': False,
        'current_offset': 0,
        'error': None,
        'captcha_detected': False,
        'response_too_small': False,
        'response_size_kb': 0
    }
    
    # Extract offset from URL if present
    if "offset=" in url:
        try:
            result['current_offset'] = int(url.split("offset=")[1].split("&")[0])
        except ValueError:
            result['current_offset'] = 0
    
    try:
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries and not result['restaurants_found']:
            retry_count += 1  # Increment at the beginning to avoid infinite loops
            
            if retry_count > 1:
                time.sleep(2 * (retry_count - 1))  # Exponential backoff
            
            response = client.request_spider_api("detailed", url)
            
            if response and len(response) > 0 and response[0] and response[0].get("error") is None:
                # Check if we got a valid response
                if "json_data" in response[0]:
                    json_data = response[0]["json_data"]
                    
                    if "other_scripts" in json_data:
                        for script in json_data["other_scripts"]:
                            if isinstance(script, dict) and "itemListOrder" in script:
                                item_list = script.get("itemListElement", [])
                                
                                if len(item_list) > 0:
                                    result['restaurants_found'] = True
                                    result['should_remove_url'] = False  # Don't remove URLs with restaurants!
                                    
                                    for list_item in item_list:
                                        # Skip restaurants from wrong locations if validation is enabled
                                        if expected_geo_ids:
                                            # Check if the restaurant URL contains a geo ID that matches expected ones
                                            restaurant_url = ""
                                            if "item" in list_item and isinstance(list_item["item"], dict):
                                                restaurant_url = list_item["item"].get("url", "")
                                            elif "url" in list_item:
                                                restaurant_url = list_item.get("url", "")
                                            
                                            # Extract geo ID from restaurant URL
                                            if restaurant_url and "-g" in restaurant_url:
                                                try:
                                                    # TripAdvisor URLs typically have format: .../Restaurant_Review-g[geo_id]-...
                                                    geo_part = restaurant_url.split("-g")[1].split("-")[0]
                                                    if geo_part not in expected_geo_ids:
                                                        print(f"   ⚠️ Skipping restaurant from unexpected location (geo={geo_part})")
                                                        continue
                                                except (IndexError, ValueError):
                                                    pass  # If we can't parse, process anyway
                                        
                                        # Handle items with full data (nested in "item" key)
                                        if "item" in list_item and isinstance(list_item["item"], dict):
                                            list_item["item"]["city_geoname_id"] = geoname_id
                                            api_result = add_restaurant_basic_info(list_item["item"])
                                            if api_result.get("success"):
                                                result['successful_restaurants'] += 1
                                            else:
                                                result['failed_restaurants'] += 1
                                        # Handle items with minimal data (name and url at top level)
                                        elif "name" in list_item and "url" in list_item:
                                            # Create a minimal restaurant entry with city_geoname_id
                                            minimal_data = {
                                                "name": list_item.get("name", ""),
                                                "url": list_item.get("url", ""),
                                                "city_geoname_id": geoname_id,
                                                # Add empty address structure to ensure city_geoname_id is processed
                                                "address": {}
                                            }
                                            api_result = add_restaurant_basic_info(minimal_data)
                                            if api_result.get("success"):
                                                result['successful_restaurants'] += 1
                                            else:
                                                result['failed_restaurants'] += 1
                
                # Validate response and check for issues
                if not result['restaurants_found']:
                    # Check for CAPTCHA
                    if detect_captcha(response[0]):
                        result['captcha_detected'] = True
                        result['error'] = "CAPTCHA detected"
                        print(f"⚠️  CAPTCHA detected for URL: {url}")
                        break  # Don't retry on CAPTCHA
                    
                    # Check response size
                    if not validate_response_size(response[0]):
                        result['response_too_small'] = True
                        # Calculate actual size for logging
                        content = response[0].get('content', '')
                        json_data_str = str(response[0].get('json_data', {}))
                        result['response_size_kb'] = (len(content) + len(json_data_str)) / 1024
                        
                        if retry_count < max_retries:
                            print(f"⚠️  Response too small ({result['response_size_kb']:.1f}KB), retrying... (attempt {retry_count}/{max_retries})")
                            continue
                        else:
                            result['error'] = f"Response too small ({result['response_size_kb']:.1f}KB)"
                    
                    # Check status code
                    if "status" in response[0]:
                        status_code = response[0].get("status", 200)
                        if status_code == 429:
                            result['error'] = "Rate limited (429)"
                            break  # Don't retry on rate limit
                        elif status_code != 200 and retry_count < max_retries:
                            continue
        
        # Handle case where no restaurants found after all retries
        if not result['restaurants_found']:
            # Only remove URL if we're certain it's an empty result page
            # Don't remove on CAPTCHA, rate limiting, or suspicious responses
            if (response and len(response) > 0 and response[0] and 
                response[0].get('error') is None and 
                not result['captcha_detected'] and 
                not result['response_too_small']):
                
                status_code = response[0].get("status", 200)
                # Only remove if we got a valid 200 response with proper structure but no data
                if status_code == 200 and validate_response_size(response[0]):
                    # Double-check that the page structure exists but is empty
                    json_data = response[0].get("json_data", {})
                    if "other_scripts" in json_data:
                        # Structure exists but no restaurants - safe to remove
                        result['should_remove_url'] = True
                    else:
                        # Structure missing - might be blocked or error page
                        result['should_remove_url'] = False
                else:
                    # Don't remove on non-200 status or small responses
                    result['should_remove_url'] = False
            else:
                # Don't remove on error, CAPTCHA, or suspicious response
                result['should_remove_url'] = False
    
    except Exception as e:
        result['error'] = str(e)
        # Don't remove on exception, might be temporary issue
        result['should_remove_url'] = False
    
    return result


def add_request_log(url: str, status: str, response_time: float) -> None:
    """Logs the request details to a file."""
    log_entry = {
        "url": url,
        "status": status,
        "response_time": response_time,
    }

    with open("request_log.json", "a") as log_file:
        log_file.write(json.dumps(log_entry) + "\n")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Scrape TripAdvisor restaurant data for a specific country'
    )
    parser.add_argument(
        '--country', '-c',
        type=str,
        default='NL',
        help='ISO 2-letter country code (e.g., US, FR, DE). Default: NL'
    )
    parser.add_argument(
        '--geo-id', '-g',
        type=int,
        default=None,
        help='Specific TripAdvisor geo ID to process (overrides country option)'
    )
    parser.add_argument(
        '--status', '-s',
        type=str,
        default='pending',
        choices=['pending', 'completed', 'in_progress'],
        help='Filter URLs by status. Default: pending'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Limit the number of cities to process'
    )
    parser.add_argument(
        '--continuous', '-C',
        action='store_true',
        help='Continue processing until all URLs are scraped from database'
    )
    parser.add_argument(
        '--max-iterations', '-m',
        type=int,
        default=10,
        help='Maximum number of iterations in continuous mode (default: 10)'
    )
    
    args = parser.parse_args()
    country = args.country.upper() if not args.geo_id else None
    
    # Print configuration
    if args.geo_id:
        print(f"Starting restaurant scraping for specific TripAdvisor geo_id: {args.geo_id}")
    else:
        print(f"Starting restaurant scraping for country: {country}")
    print(f"URL status filter: {args.status}")
    if args.limit and not args.geo_id:
        print(f"City limit: {args.limit}")
    if args.continuous:
        print("Continuous mode: Will process until database is empty")
    
    # Main processing loop
    iteration = 0
    total_processed_overall = 0
    total_successful_overall = 0
    total_failed_overall = 0
    urls_removed_overall = 0
    
    # Get expected TripAdvisor geo IDs for validation
    expected_geo_ids = []
    resolved_geoname_id = None  # To store the resolved geoname_id for tripadvisor_geo_id
    
    if args.geo_id:
        # For specific tripadvisor_geo_id, lookup the corresponding geoname_id
        resolved_geoname_id = get_geoname_id_from_tripadvisor_geo_id(args.geo_id)
        if not resolved_geoname_id:
            print(f"❌ Could not find geoname_id for TripAdvisor geo_id: {args.geo_id}")
            exit(1)  # Exit with error code instead of return
        # No location validation for specific geo_id
        expected_geo_ids = []
        print(f"Will process geoname_id: {resolved_geoname_id} (no location validation)")
    else:
        # Get expected TripAdvisor geo IDs for the country (for validation)
        expected_geo_ids = get_tripadvisor_geo_ids_by_country(country)
        print(f"Found {len(expected_geo_ids)} valid TripAdvisor geo IDs for {country}")
    
    while True:
        iteration += 1
        
        if iteration > 1:
            print(f"\n{'='*60}")
            print(f"ITERATION {iteration}: Checking for remaining URLs")
            print(f"{'='*60}")
            
            # Check max iterations
            if args.continuous and iteration > args.max_iterations:
                print(f"⚠️  Reached maximum iterations ({args.max_iterations}). Stopping.")
                break
        
        # Get geoname IDs based on mode (specific geo_id or country)
        if args.geo_id:
            # Use the resolved geoname_id
            geoname_ids = [resolved_geoname_id]
            print(f"Processing single geoname_id: {resolved_geoname_id} (from TripAdvisor geo_id: {args.geo_id})")
        else:
            geoname_ids = get_geoname_ids_by_country(country)
            print(f"Found {len(geoname_ids)} cities with restaurant data in {country}")
            
            if args.limit and len(geoname_ids) > args.limit:
                geoname_ids = geoname_ids[:args.limit]
                print(f"Limited to first {args.limit} cities")

        if not geoname_ids:
            if args.geo_id:
                print(f"No data found for TripAdvisor geo_id: {args.geo_id}")
            else:
                print("No geoname IDs found for the specified country.")
            break
        
        urls = get_city_restaurant_urls_with_status(geoname_ids, args.status)
        total_url_count = sum(len(url_list) for url_list in urls.values())
        
        if total_url_count == 0:
            print(f"✅ All URLs have been processed! No {args.status} URLs remaining in database.")
            break
        
        if args.geo_id:
            print(f"Retrieved {total_url_count} {args.status} URLs for TripAdvisor geo_id {args.geo_id}")
        else:
            print(f"Retrieved {total_url_count} {args.status} URLs from {len(urls)} cities in {country}")

        total_urls = sum(len(url_list) for url_list in urls.values())
        processed_urls = 0
        successful_restaurants = 0
        failed_restaurants = 0
        skipped_urls = 0
        urls_removed_this_iteration = 0
        
        print(f"\n{'='*60}")
        print(f"PROCESSING {total_urls} URLs FROM {len(urls)} CITIES")
        print("Using 5 concurrent workers for faster processing")
        print(f"{'='*60}")
        
        for geoname_id in urls:
            city_urls = urls[geoname_id]
            print(f"\n🏙️  Processing geoname_id: {geoname_id} ({len(city_urls)} URLs)")
            
            # Process all URLs without skipping based on offset
            urls_to_process = city_urls
            
            if not urls_to_process:
                continue
            
            # Process URLs concurrently in batches of 5
            batch_size = 5
            for i in range(0, len(urls_to_process), batch_size):
                batch = urls_to_process[i:i+batch_size]
                
                print(f"\n📦 Processing batch {i//batch_size + 1} ({len(batch)} URLs)")
                
                # Process batch concurrently
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_url = {
                        executor.submit(process_single_url, url_tuple, geoname_id, expected_geo_ids): url_tuple 
                        for url_tuple in batch
                    }
                    
                    batch_results = []
                    for future in as_completed(future_to_url):
                        try:
                            result = future.result()
                            batch_results.append(result)
                            processed_urls += 1
                            
                            # Update counters
                            successful_restaurants += result['successful_restaurants']
                            failed_restaurants += result['failed_restaurants']
                            
                            # Enhanced output with more details
                            offset_info = f" (offset={result['current_offset']})" if result['current_offset'] > 0 else ""
                            if result['restaurants_found']:
                                print(f"✓ [{processed_urls}/{total_urls}] {result['url']}{offset_info} → {result['successful_restaurants']} restaurants")
                            elif result['captcha_detected']:
                                print(f"🔒 [{processed_urls}/{total_urls}] {result['url']}{offset_info} → CAPTCHA detected (keeping for retry)")
                            elif result['response_too_small']:
                                print(f"⚠️  [{processed_urls}/{total_urls}] {result['url']}{offset_info} → Response too small ({result['response_size_kb']:.1f}KB)")
                            elif result['error']:
                                print(f"✗ [{processed_urls}/{total_urls}] {result['url']}{offset_info} → Error: {result['error']}")
                            else:
                                print(f"○ [{processed_urls}/{total_urls}] {result['url']}{offset_info} → No results (empty page)")
                            
                            # Handle URL removal - remove successfully processed URLs OR confirmed empty pages
                            if result['restaurants_found'] and result['successful_restaurants'] > 0:
                                # Remove URL after successful processing
                                if remove_city_restaurant_url(result['url']):
                                    urls_removed_overall += 1
                                    urls_removed_this_iteration += 1
                                    print("   → Removed from queue (successfully processed)")
                            elif result['should_remove_url']:
                                # Remove confirmed empty pages
                                if remove_city_restaurant_url(result['url']):
                                    urls_removed_overall += 1
                                    urls_removed_this_iteration += 1
                                    print("   → Removed from queue (confirmed empty page)")
                            elif result['captcha_detected'] or result['response_too_small']:
                                # Log suspicious responses for monitoring
                                with open("suspicious_responses.log", "a") as log_file:
                                    log_entry = {
                                        "timestamp": datetime.now().isoformat(),
                                        "url": result['url'],
                                        "captcha": result['captcha_detected'],
                                        "too_small": result['response_too_small'],
                                        "size_kb": result['response_size_kb'],
                                        "error": result['error']
                                    }
                                    log_file.write(json.dumps(log_entry) + "\n")
                            
                        except Exception as e:
                            url_tuple = future_to_url[future]
                            print(f"✗ Error processing {url_tuple[0]}: {e}")
                            failed_restaurants += 1
        
        # Update overall counters
        total_processed_overall += processed_urls
        total_successful_overall += successful_restaurants
        total_failed_overall += failed_restaurants
        
        # Iteration summary
        print(f"\n{'='*60}")
        print(f"ITERATION {iteration} SUMMARY")
        print(f"{'='*60}")
        print(f"📈 URLs processed: {processed_urls}")
        print(f"✅ Successful restaurants: {successful_restaurants}")
        print(f"❌ Failed restaurants: {failed_restaurants}")
        print(f"⏭️  URLs skipped: {skipped_urls}")
        print(f"🗑️  URLs removed from queue: {urls_removed_this_iteration}")
        print(f"🔄 URLs kept for retry: {processed_urls - urls_removed_this_iteration}")
        
        # If not in continuous mode, break after first iteration
        if not args.continuous:
            break
    
    # Final overall summary
    print(f"\n{'='*60}")
    print("FINAL OVERALL SUMMARY")
    print(f"{'='*60}")
    print(f"📈 Total URLs processed: {total_processed_overall}")
    print(f"✅ Total successful restaurants: {total_successful_overall}")
    print(f"❌ Total failed restaurants: {total_failed_overall}")
    print(f"🗑️  Total URLs removed from queue: {urls_removed_overall}")
    print(f"🎯 Overall success rate: {(total_successful_overall/(total_successful_overall+total_failed_overall)*100):.1f}%" if (total_successful_overall+total_failed_overall) > 0 else "N/A")
                            



# 1. get geoname_ids_by_country: Fetches geoname IDs for cities in a specified country with TripAdvisor restaurant data.
# 2. get urls from local sqlite database for the specifued geoname_id's
# 3. for the retrieved URLs, request the spider API to get the restaurants lists data 
# 4. parse the restaurant URLs from the API response
# 5. add the restaurant urls to the API https://127.0.0.0.8000/api/restaurants/