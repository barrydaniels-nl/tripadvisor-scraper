#!/usr/bin/env python3
"""
Test script for the Restaurant Scraper Lambda function.
Run this locally to test the Lambda handler.
"""

import json
import os
from unittest.mock import patch, MagicMock
import sys

# Add current directory to path to import the Lambda handler
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_lambda_handler():
    """Test the Lambda handler with mock data."""

    # Set up environment variables for testing
    os.environ['S3_BUCKET'] = 'test-bucket'
    os.environ['API_ENDPOINT'] = 'https://viberoam.ai/api/restaurants/random/?country=NL&never_scraped=1'

    # Import the handler after setting environment variables
    from AWS_scrape_restaurant_data import lambda_handler

    # Create test event
    test_event = {}
    test_context = MagicMock()

    # Mock restaurant data
    mock_restaurant = {
        "id": 12345,
        "name": "Test Restaurant",
        "tripadvisor_detail_page": "https://www.tripadvisor.com/Restaurant_Review-test.html",
        "city": {
            "name": "Amsterdam",
            "country": {
                "name": "Netherlands"
            }
        }
    }

    # Mock the external calls
    with patch('AWS_scrape_restaurant_data.fetch_random_restaurant') as mock_fetch:
        with patch('AWS_scrape_restaurant_data.scrape_restaurant_data') as mock_scrape:
            with patch('AWS_scrape_restaurant_data.upload_to_s3') as mock_upload:

                # Configure mocks
                mock_fetch.return_value = mock_restaurant
                mock_scrape.return_value = {
                    'restaurant_id': 12345,
                    'name': 'Test Restaurant',
                    'success': True,
                    'data': {'rating': '4.5', 'review_count': '100'}
                }
                mock_upload.return_value = 'scraped_data/20240101_120000/12345_Test_Restaurant.json'

                # Call the handler
                response = lambda_handler(test_event, test_context)

                # Verify response
                assert response['statusCode'] == 200
                body = json.loads(response['body'])
                assert body['success'] == True
                assert body['restaurant_name'] == 'Test Restaurant'
                assert 's3_key' in body

                print("✅ Test passed: Lambda handler works correctly")
                print(f"Response: {json.dumps(response, indent=2)}")

def test_error_handling():
    """Test error handling in the Lambda function."""

    from AWS_scrape_restaurant_data import lambda_handler

    test_event = {}
    test_context = MagicMock()

    # Test with no restaurant available
    with patch('AWS_scrape_restaurant_data.fetch_random_restaurant') as mock_fetch:
        mock_fetch.return_value = None

        response = lambda_handler(test_event, test_context)

        assert response['statusCode'] == 404
        body = json.loads(response['body'])
        assert 'error' in body

        print("✅ Test passed: Error handling works correctly")
        print(f"Error Response: {json.dumps(response, indent=2)}")

def test_scraping_functions():
    """Test individual scraping functions."""

    from AWS_scrape_restaurant_data import extract_basic_info, extract_json_ld
    from bs4 import BeautifulSoup

    # Test HTML with sample data
    html = """
    <html>
        <head>
            <script type="application/ld+json">
            {
                "@type": "Restaurant",
                "name": "Test Restaurant",
                "aggregateRating": {
                    "@type": "AggregateRating",
                    "ratingValue": "4.5",
                    "reviewCount": "123"
                }
            }
            </script>
        </head>
        <body>
            <span class="rating">4.5</span>
            <span class="review-count">123 reviews</span>
        </body>
    </html>
    """

    soup = BeautifulSoup(html, 'html.parser')

    # Test JSON-LD extraction
    json_ld = extract_json_ld(soup)
    assert json_ld is not None
    assert json_ld['@type'] == 'Restaurant'
    assert json_ld['name'] == 'Test Restaurant'

    print("✅ Test passed: Scraping functions work correctly")
    print(f"Extracted JSON-LD: {json.dumps(json_ld, indent=2)}")

def main():
    """Run all tests."""
    print("Running Lambda function tests...\n")

    try:
        test_lambda_handler()
        print()
        test_error_handling()
        print()
        test_scraping_functions()
        print("\n✅ All tests passed!")

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()