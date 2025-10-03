import json
import boto3
import requests
from datetime import datetime
import os
from urllib.parse import urlparse
import re
from bs4 import BeautifulSoup
import time
import random

# Initialize S3 client
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET', 'restaurant-scraper-data')
API_ENDPOINT = os.environ.get('API_ENDPOINT', 'https://viberoam.ai/api/restaurants/random/?country=NL&never_scraped=1')
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
TIMEOUT = int(os.environ.get('TIMEOUT', '30'))

def lambda_handler(event, context):
    """
    AWS Lambda handler function for scraping restaurant data.

    Args:
        event: Lambda event object
        context: Lambda context object

    Returns:
        dict: Response with status and details
    """
    try:
        # Fetch restaurant to scrape
        restaurant = fetch_random_restaurant()

        if not restaurant:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No restaurant available to scrape'})
            }

        # Scrape restaurant data
        scraped_data = scrape_restaurant_data(restaurant)

        # Upload to S3
        s3_key = upload_to_s3(scraped_data, restaurant)

        # Update scraping status via API if endpoint is available
        if os.environ.get('UPDATE_API_ENDPOINT'):
            update_scraping_status(restaurant['id'], 'success')

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'restaurant_name': restaurant['name'],
                'restaurant_id': restaurant.get('id'),
                's3_key': s3_key,
                'timestamp': datetime.utcnow().isoformat()
            })
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")

        # Update scraping status as failed
        if restaurant and os.environ.get('UPDATE_API_ENDPOINT'):
            update_scraping_status(restaurant.get('id'), 'failed', str(e))

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'type': type(e).__name__
            })
        }

def fetch_random_restaurant():
    """
    Fetch a random restaurant from the API that hasn't been scraped yet.

    Returns:
        dict: Restaurant data or None if failed
    """
    try:
        response = requests.get(
            API_ENDPOINT,
            timeout=TIMEOUT,
            headers={'User-Agent': 'AWS-Lambda-Scraper/1.0'}
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching restaurant: HTTP {response.status_code}")
            return None

    except Exception as e:
        print(f"Error fetching restaurant: {e}")
        return None

def scrape_restaurant_data(restaurant):
    """
    Scrape data from TripAdvisor for the given restaurant.
    This is a simplified version - you may need to add more sophisticated scraping logic.

    Args:
        restaurant: Restaurant data from API

    Returns:
        dict: Scraped data
    """
    scraped_data = {
        'restaurant_id': restaurant.get('id'),
        'name': restaurant.get('name'),
        'tripadvisor_url': restaurant.get('tripadvisor_detail_page'),
        'city': restaurant.get('city', {}).get('name'),
        'country': restaurant.get('city', {}).get('country', {}).get('name', 'Netherlands'),
        'scraped_at': datetime.utcnow().isoformat(),
        'data': {}
    }

    # Get TripAdvisor page
    url = restaurant.get('tripadvisor_detail_page')
    if not url:
        raise ValueError("No TripAdvisor URL found for restaurant")

    try:
        # Add delay to avoid rate limiting
        time.sleep(random.uniform(1, 3))

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }

        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract basic information (simplified version)
        scraped_data['data'] = extract_basic_info(soup)

        # Look for JSON-LD data
        json_ld = extract_json_ld(soup)
        if json_ld:
            scraped_data['json_ld_data'] = json_ld

        scraped_data['success'] = True

    except Exception as e:
        print(f"Error scraping restaurant data: {e}")
        scraped_data['error'] = str(e)
        scraped_data['success'] = False

    return scraped_data

def extract_basic_info(soup):
    """
    Extract basic information from the page.

    Args:
        soup: BeautifulSoup object

    Returns:
        dict: Extracted data
    """
    data = {}

    try:
        # Try to extract rating
        rating_elem = soup.find('span', class_=re.compile(r'.*rating.*'))
        if rating_elem:
            data['rating'] = rating_elem.get_text(strip=True)

        # Try to extract review count
        review_elem = soup.find('span', class_=re.compile(r'.*review.*count.*'))
        if review_elem:
            data['review_count'] = review_elem.get_text(strip=True)

        # Try to extract address
        address_elem = soup.find('span', class_=re.compile(r'.*address.*'))
        if address_elem:
            data['address'] = address_elem.get_text(strip=True)

        # Try to extract cuisine type
        cuisine_elem = soup.find('span', text=re.compile(r'CUISINES'))
        if cuisine_elem:
            cuisine_value = cuisine_elem.find_next_sibling()
            if cuisine_value:
                data['cuisine'] = cuisine_value.get_text(strip=True)

        # Try to extract price range
        price_elem = soup.find('span', text=re.compile(r'PRICE RANGE'))
        if price_elem:
            price_value = price_elem.find_next_sibling()
            if price_value:
                data['price_range'] = price_value.get_text(strip=True)

    except Exception as e:
        print(f"Error extracting basic info: {e}")

    return data

def extract_json_ld(soup):
    """
    Extract JSON-LD structured data from the page.

    Args:
        soup: BeautifulSoup object

    Returns:
        dict: JSON-LD data or None
    """
    try:
        json_ld_scripts = soup.find_all('script', type='application/ld+json')

        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                # Look for Restaurant type
                if isinstance(data, dict):
                    if data.get('@type') == 'Restaurant':
                        return data
                    elif data.get('@graph'):
                        # Check in graph
                        for item in data['@graph']:
                            if item.get('@type') == 'Restaurant':
                                return item
            except json.JSONDecodeError:
                continue

    except Exception as e:
        print(f"Error extracting JSON-LD: {e}")

    return None

def upload_to_s3(scraped_data, restaurant):
    """
    Upload scraped data to S3 bucket.

    Args:
        scraped_data: Data to upload
        restaurant: Restaurant information

    Returns:
        str: S3 key where data was uploaded
    """
    # Generate S3 key
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    restaurant_name_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', restaurant.get('name', 'unknown'))
    restaurant_id = restaurant.get('id', 'no_id')

    s3_key = f"scraped_data/{timestamp}/{restaurant_id}_{restaurant_name_clean}.json"

    # Convert data to JSON
    json_data = json.dumps(scraped_data, indent=2, ensure_ascii=False)

    # Upload to S3
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'restaurant_id': str(restaurant.get('id', '')),
                'restaurant_name': restaurant.get('name', ''),
                'scraped_at': datetime.utcnow().isoformat()
            }
        )

        print(f"Successfully uploaded to S3: {s3_key}")
        return s3_key

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise

def update_scraping_status(restaurant_id, status, error_message=None):
    """
    Update the scraping status via API (if configured).

    Args:
        restaurant_id: ID of the restaurant
        status: 'success' or 'failed'
        error_message: Optional error message if failed
    """
    update_endpoint = os.environ.get('UPDATE_API_ENDPOINT')
    if not update_endpoint:
        return

    try:
        payload = {
            'restaurant_id': restaurant_id,
            'status': status,
            'scraped_at': datetime.utcnow().isoformat()
        }

        if error_message:
            payload['error'] = error_message

        response = requests.post(
            update_endpoint,
            json=payload,
            timeout=10
        )

        if response.status_code == 200:
            print(f"Successfully updated scraping status for restaurant {restaurant_id}")
        else:
            print(f"Failed to update status: HTTP {response.status_code}")

    except Exception as e:
        print(f"Error updating scraping status: {e}")