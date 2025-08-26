import json
import asyncio
from playwright.async_api import async_playwright
import loadenv 

loadenv.load_dotenv()

async def capture_graphql_with_playwright(url, proxy=None):
    """
    Capture GraphQL responses using Playwright with network interception
    """
    graphql_responses = []
    
    async with async_playwright() as p:
        # Browser launch options
        launch_options = {
            'headless': False  # Set to True for headless mode
        }
        
        # Add proxy if provided
        if proxy:
            launch_options['proxy'] = {
                'server': proxy,
                # Optional: add authentication if needed
                # 'username': 'user',
                # 'password': 'pass'
            }
        
        browser = await p.chromium.launch(**launch_options)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Set up response listener
        async def handle_response(response):
            url = response.url
            # Check if it's a GraphQL endpoint
            if 'graphql' in url.lower() or '/gql' in url:
                try:
                    # Get response body
                    body = await response.body()
                    json_body = json.loads(body.decode('utf-8'))
                    
                    graphql_data = {
                        'url': url,
                        'status': response.status,
                        'headers': await response.all_headers(),
                        'body': json_body,
                        'request': {
                            'method': response.request.method,
                            'headers': await response.request.all_headers(),
                            'post_data': response.request.post_data
                        }
                    }
                    graphql_responses.append(graphql_data)
                    print(f"Captured GraphQL response from: {url}")
                    
                    # Optionally print the GraphQL query/response
                    if response.request.post_data:
                        request_body = json.loads(response.request.post_data)
                        print(f"Query: {request_body.get('query', 'N/A')[:100]}...")
                        
                except Exception as e:
                    print(f"Error processing response: {e}")
        
        # Attach the response listener
        page.on('response', handle_response)
        
        # Navigate to the website
        await page.goto(url, wait_until='networkidle')
        
        # Optional: wait for specific elements or additional time
        await page.wait_for_timeout(5000)
        
        # Optional: interact with the page to trigger more GraphQL requests
        # await page.click('button#load-more')
        # await page.wait_for_timeout(2000)
        
        await browser.close()
    
    return graphql_responses

# Synchronous wrapper for easier use
def capture_graphql_sync(url, proxy=None):
    """
    Synchronous wrapper for the async function
    """
    return asyncio.run(capture_graphql_with_playwright(url, proxy))

# Usage example
if __name__ == "__main__":
    # Without proxy
    responses = capture_graphql_sync("https://example.com")
    
    # With proxy
    # responses = capture_graphql_sync("https://example.com", proxy="http://proxy-server:8080")
    
    # With authenticated proxy
    # responses = capture_graphql_sync("https://example.com", proxy="http://username:password@proxy-server:8080")
    
    # Save responses to file
    with open('graphql_responses_playwright.json', 'w') as f:
        json.dump(responses, f, indent=2, default=str)
    
    print(f"Captured {len(responses)} GraphQL responses")
    
    # Print first response details
    if responses:
        print("\nFirst GraphQL Response:")
        print(json.dumps(responses[0], indent=2, default=str)[:500])