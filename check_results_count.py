import requests
import argparse
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_cities_with_restaurant_data(country_code: str = None) -> List[Dict]:
    """
    Fetch cities with TripAdvisor restaurant data from the API.
    
    Args:
        country_code: Optional ISO 2-letter country code to filter cities
        
    Returns:
        List of cities with geoname_id and tripadvisor_restaurants_results
    """
    all_cities = []
    page = 1
    page_size = 1000
    
    print("Fetching cities with restaurant data" + 
          (f" for country: {country_code}" if country_code else ""))
    
    while True:
        # Build URL with optional country filter
        url = (
            f"http://127.0.0.1:8000/api/cities/search/"
            f"?page={page}"
            f"&page_size={page_size}"
            f"&tripadvisor_geo_id_is_null=false"
            f"&min_restaurants=1"
        )
        
        if country_code:
            url += f"&country={country_code}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                
                # Handle different response structures - prioritize 'results' for DRF pagination
                items = []
                if isinstance(data, dict):
                    if 'results' in data:
                        items = data.get('results', [])
                    elif 'items' in data:
                        items = data.get('items', [])
                    elif 'data' in data:
                        items = data.get('data', [])
                    else:
                        # Try to find a list value in the dict
                        for _, value in data.items():
                            if isinstance(value, list) and len(value) > 0:
                                if isinstance(value[0], dict) and 'geoname_id' in value[0]:
                                    items = value
                                    break
                elif isinstance(data, list):
                    items = data
                
                if not items:
                    print(f"No more cities found on page {page}")
                    break
                
                # Filter cities that have required data
                valid_cities = []
                for city in items:
                    if (city.get('geoname_id') and 
                        city.get('tripadvisor_restaurants_results') is not None):
                        # Handle nested country structure
                        country_code = 'Unknown'
                        if 'country_code' in city:
                            country_code = city['country_code']
                        elif 'country' in city and isinstance(city['country'], dict):
                            country_code = city['country'].get('code', 'Unknown')
                        elif 'country' in city and isinstance(city['country'], str):
                            country_code = city['country']
                        
                        valid_cities.append({
                            'geoname_id': city.get('geoname_id'),
                            'name': city.get('name', 'Unknown'),
                            'tripadvisor_restaurants_results': city.get('tripadvisor_restaurants_results'),
                            'country': country_code
                        })
                
                if valid_cities:
                    all_cities.extend(valid_cities)
                    print(f"Page {page}: Found {len(valid_cities)} cities "
                          f"(total so far: {len(all_cities)})")
                
                # Check if we should continue
                if len(items) < page_size:
                    print("Reached end of results")
                    break
                
                page += 1
                
            else:
                print(f"API error: {response.status_code} - {response.text}")
                break
                
        except Exception as e:
            print(f"Error fetching cities: {e}")
            break
    
    return all_cities


def get_restaurant_count_for_geoname(geoname_id: int) -> int:
    """
    Get the actual count of restaurants in the database for a geoname_id.
    Fetches all pages to get accurate total count.
    
    Args:
        geoname_id: The geoname ID to count restaurants for
        
    Returns:
        Number of restaurants found in the database
    """
    try:
        # First, try to get the count from pagination metadata
        url = f"http://127.0.0.1:8000/api/restaurants/search/?geoname_id={geoname_id}&page_size=1"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Handle different response structures - prioritize count field
            if isinstance(data, dict):
                # Check for total count in pagination metadata
                if 'count' in data:
                    return data.get('count', 0)
                elif 'total' in data:
                    return data.get('total', 0)
                elif 'total_count' in data:
                    return data.get('total_count', 0)
                elif 'pagination' in data:
                    pagination = data.get('pagination', {})
                    if 'total' in pagination:
                        return pagination.get('total', 0)
                    elif 'count' in pagination:
                        return pagination.get('count', 0)
                
                # If no count field, we need to paginate through all results
                all_items = []
                page = 1
                page_size = 100
                
                while True:
                    url = f"http://127.0.0.1:8000/api/restaurants/search/?geoname_id={geoname_id}&page={page}&page_size={page_size}"
                    response = requests.get(url)
                    
                    if response.status_code != 200:
                        break
                        
                    data = response.json()
                    
                    # Extract items from response
                    items = []
                    if 'items' in data:
                        items = data.get('items', [])
                    elif 'results' in data:
                        items = data.get('results', [])
                    elif 'data' in data:
                        items = data.get('data', [])
                    elif isinstance(data, list):
                        items = data
                    else:
                        # Try to find a list value
                        for key, value in data.items():
                            if isinstance(value, list) and len(value) > 0:
                                if isinstance(value[0], dict) and 'tripadvisor_id' in value[0]:
                                    items = value
                                    break
                    
                    if not items:
                        break
                        
                    all_items.extend(items)
                    
                    # Check if we should continue
                    if len(items) < page_size:
                        break
                        
                    page += 1
                
                return len(all_items)
            elif isinstance(data, list):
                return len(data)
            else:
                return 0
        else:
            print(f"Error fetching restaurant count for {geoname_id}: "
                  f"{response.status_code}")
            return 0
            
    except Exception as e:
        print(f"Error counting restaurants for {geoname_id}: {e}")
        return 0


def validate_single_city(city: Dict, city_index: int, total_cities: int) -> Dict:
    """
    Validate restaurant count for a single city.
    
    Args:
        city: City dictionary with expected counts
        city_index: Current city index for progress tracking
        total_cities: Total number of cities being processed
        
    Returns:
        Validation result dictionary
    """
    geoname_id = city['geoname_id']
    expected_count = city['tripadvisor_restaurants_results']
    city_name = city['name']
    
    actual_count = get_restaurant_count_for_geoname(geoname_id)
    
    # Calculate 5% tolerance
    tolerance = max(1, int(expected_count * 0.05))  # At least 1
    min_valid = expected_count - tolerance
    max_valid = expected_count + tolerance
    
    is_valid = min_valid <= actual_count <= max_valid
    difference = actual_count - expected_count
    difference_pct = (difference / expected_count * 100) if expected_count > 0 else 0
    
    # Print compact one-line result
    status_icon = "✓" if is_valid else "✗"
    print(f"{status_icon} [{city_index:3d}/{total_cities}] {city_name:30s} "
          f"[ACT] {actual_count:5d} [EXP] {expected_count:5d} "
          f"[DIFF] {difference:+6d} ({difference_pct:+6.1f}%) "
          f"[http://127.0.0.1:8000/api/city/{geoname_id}]")
    
    result = {
        'geoname_id': geoname_id,
        'city_name': city_name,
        'country': city['country'],
        'expected_count': expected_count,
        'actual_count': actual_count,
        'difference': difference,
        'difference_pct': difference_pct,
        'tolerance': tolerance,
        'is_valid': is_valid,
        'status': 'VALID' if is_valid else 'INVALID'
    }
    
    return result


def validate_restaurant_counts(cities: List[Dict]) -> List[Dict]:
    """
    Validate restaurant counts with 5% tolerance using concurrent processing.
    
    Args:
        cities: List of city dictionaries with expected counts
        
    Returns:
        List of validation results
    """
    results = []
    
    # Process cities concurrently with 5 workers
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        future_to_city = {
            executor.submit(validate_single_city, city, i, len(cities)): city 
            for i, city in enumerate(cities, 1)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_city):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                city = future_to_city[future]
                print(f"Error validating city {city.get('name', 'Unknown')}: {e}")
    
    # Sort results by geoname_id to maintain consistent order
    results.sort(key=lambda x: x['geoname_id'])
    
    return results


def print_summary(results: List[Dict]):
    """Print summary statistics of the validation."""
    total_cities = len(results)
    valid_cities = sum(1 for r in results if r['is_valid'])
    invalid_cities = total_cities - valid_cities
    
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total cities checked: {total_cities}")
    print(f"Valid counts: {valid_cities} ({valid_cities/total_cities*100:.1f}%)")
    print(f"Invalid counts: {invalid_cities} ({invalid_cities/total_cities*100:.1f}%)")
    
    if invalid_cities > 0:
        print("\nINVALID CITIES:")
        print("-" * 40)
        for result in results:
            if not result['is_valid']:
                print(f"{result['city_name']} ({result['country']}): "
                      f"Expected {result['expected_count']}, "
                      f"Got {result['actual_count']} "
                      f"({result['difference_pct']:+.1f}%)")
    
    # Statistics
    if results:
        total_expected = sum(r['expected_count'] for r in results)
        total_actual = sum(r['actual_count'] for r in results)
        overall_diff = total_actual - total_expected
        overall_diff_pct = (overall_diff / total_expected * 100) if total_expected > 0 else 0
        
        print("\nOVERALL STATISTICS:")
        print("-" * 40)
        print(f"Total expected restaurants: {total_expected:,}")
        print(f"Total actual restaurants: {total_actual:,}")
        print(f"Overall difference: {overall_diff:+,} ({overall_diff_pct:+.2f}%)")


def main():
    """Main function to validate restaurant counts."""
    parser = argparse.ArgumentParser(
        description='Validate restaurant counts between TripAdvisor and database'
    )
    parser.add_argument(
        '--country', '-c',
        type=str,
        default=None,
        help='ISO 2-letter country code to check (optional)'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Limit the number of cities to check'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("RESTAURANT COUNT VALIDATION")
    print("=" * 60)
    
    # Fetch cities
    cities = fetch_cities_with_restaurant_data(args.country)
    
    if not cities:
        print("No cities found to validate.")
        return
    
    if args.limit and len(cities) > args.limit:
        cities = cities[:args.limit]
        print(f"Limited to first {args.limit} cities")
    
    print(f"\nValidating restaurant counts for {len(cities)} cities...")
    print("Tolerance: ±5% difference allowed")
    print("Using 5 concurrent workers for faster processing")
    
    # Validate counts
    results = validate_restaurant_counts(cities)
    
    # Print summary
    print_summary(results)


if __name__ == "__main__":
    main()