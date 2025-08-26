import asyncio
import json
import os
from typing import List, Dict, Any
from urllib.parse import urlparse
import dotenv
from camoufox.async_api import AsyncCamoufox

# Load environment variables
dotenv.load_dotenv()


class TripAdvisorGraphQLScraper:
    def __init__(self):
        self.proxy_url = os.getenv("PROXY_URL")
        self.graphql_responses: List[Dict[str, Any]] = []
        
    def parse_proxy(self) -> Dict[str, Any]:
        """Parse proxy URL and return proxy configuration for Camoufox."""
        if not self.proxy_url:
            return {}
            
        parsed = urlparse(self.proxy_url)
        proxy_config = {
            "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        }
        
        if parsed.username and parsed.password:
            proxy_config["username"] = parsed.username
            proxy_config["password"] = parsed.password
            
        return proxy_config
    
    async def intercept_graphql_response(self, response):
        """Intercept and store GraphQL responses."""
        try:
            # Check if this is a GraphQL request to the specific endpoint
            if "data/graphql/ids" in response.url:
                # Get the response body as JSON
                try:
                    json_data = await response.json()
                    
                    # Store the response data
                    self.graphql_responses.append({
                        "url": response.url,
                        "status": response.status,
                        "data": json_data,
                        "headers": dict(response.headers)
                    })
                    
                    print(f"✓ Captured GraphQL response: {response.url}")
                    print(f"  Status: {response.status}")
                    
                    # Print a preview of the data structure
                    if json_data:
                        if isinstance(json_data, list):
                            print(f"  Response contains {len(json_data)} items")
                        elif isinstance(json_data, dict):
                            keys = list(json_data.keys())[:5]  # First 5 keys
                            print(f"  Response keys: {keys}")
                            
                except Exception as e:
                    print(f"  Could not parse JSON: {e}")
                    # Try to get text response
                    text_data = await response.text()
                    self.graphql_responses.append({
                        "url": response.url,
                        "status": response.status,
                        "data": text_data,
                        "headers": dict(response.headers)
                    })
                    
        except Exception as e:
            print(f"Error intercepting response: {e}")
    
    async def scrape_tripadvisor(self, url: str):
        """Main scraping function using Camoufox browser."""
        
        print("🦊 Starting Camoufox browser...")
        
        # Configure browser options
        browser_args = []
        proxy_config = self.parse_proxy()
        
        if proxy_config:
            print(f"🔗 Using proxy: {proxy_config['server']}")
        
        async with AsyncCamoufox(
            headless=False,  # Set to True for production
            block_images=False,  # Allow images for full page experience
            block_webrtc=True,  # Block WebRTC for privacy
            humanize=True,  # Enable human-like cursor movement
            proxy=proxy_config if proxy_config else None,
            # addons are loaded by default (ublock, bpc)
        ) as browser:
            
            print("📄 Creating new page...")
            page = await browser.new_page(viewport={"width": 1920, "height": 1080})
            
            # Set up response interceptor
            page.on("response", lambda response: asyncio.create_task(self.intercept_graphql_response(response)))
            
            print(f"🌐 Navigating to: {url}")
            
            try:
                # Navigate to the page with timeout
                await page.goto(url, wait_until="networkidle", timeout=30000)
                
                print("⏳ Waiting for content to load...")
                
                # Wait for the page to fully load and GraphQL requests to complete
                await page.wait_for_timeout(5000)  # Initial wait
                
                # Scroll to trigger lazy loading
                print("📜 Scrolling to load dynamic content...")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await page.wait_for_timeout(2000)
                
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)
                
                # Try clicking on reviews tab if present to load more data
                try:
                    reviews_tab = page.locator('a[href*="Reviews"]').first
                    if await reviews_tab.is_visible():
                        print("🖱️ Clicking on Reviews tab...")
                        await reviews_tab.click()
                        await page.wait_for_timeout(3000)
                except:
                    pass
                
                # Final wait for any remaining requests
                await page.wait_for_timeout(3000)
                
                print(f"\n✅ Scraping complete!")
                print(f"📊 Captured {len(self.graphql_responses)} GraphQL responses")
                
            except Exception as e:
                print(f"❌ Error during scraping: {e}")
                
            finally:
                await page.close()
        
        return self.graphql_responses
    
    def save_responses(self, filename: str = "graphql_responses.json"):
        """Save captured GraphQL responses to a JSON file."""
        if self.graphql_responses:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.graphql_responses, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved {len(self.graphql_responses)} responses to {filename}")
        else:
            print("⚠️ No responses to save")
    
    def analyze_responses(self):
        """Analyze and summarize captured GraphQL responses."""
        if not self.graphql_responses:
            print("No GraphQL responses captured")
            return
        import pdb; pdb.set_trace()  # Debugging breakpoint
        print("\n" + "="*50)
        print("📊 GRAPHQL RESPONSE ANALYSIS")
        print("="*50)
        
        for i, response in enumerate(self.graphql_responses, 1):
            print(f"\n[Response {i}]")
            print(f"URL: {response['url']}")
            print(f"Status: {response['status']}")
            
            data = response.get('data')
            if isinstance(data, dict):
                # Analyze the structure
                self._print_data_structure(data, max_depth=3)
            elif isinstance(data, list):
                print(f"List with {len(data)} items")
                if data:
                    print("First item structure:")
                    self._print_data_structure(data[0], max_depth=3)
            else:
                print(f"Data type: {type(data)}")
    
    def _print_data_structure(self, data: Any, indent: int = 0, max_depth: int = 3):
        """Recursively print data structure."""
        if indent >= max_depth:
            return
            
        if isinstance(data, dict):
            for key, value in list(data.items())[:10]:  # Limit to first 10 keys
                print("  " * indent + f"├─ {key}: ", end="")
                if isinstance(value, (dict, list)):
                    print(f"{type(value).__name__} ({len(value)} items)")
                    if indent < max_depth - 1:
                        self._print_data_structure(value, indent + 1, max_depth)
                else:
                    value_str = str(value)[:50]
                    if len(str(value)) > 50:
                        value_str += "..."
                    print(f"{type(value).__name__} - {value_str}")


async def main():
    """Main function to run the scraper."""
    
    # URL to scrape
    url = "https://www.tripadvisor.com/Restaurant_Review-g188575-d807608-Reviews-Cafe_Sjiek-Maastricht_Limburg_Province.html"
    
    # Create scraper instance
    scraper = TripAdvisorGraphQLScraper()
    
    # Run the scraper
    responses = await scraper.scrape_tripadvisor(url)
    
    # Analyze the responses
    scraper.analyze_responses()
    
    # Save responses to file
    scraper.save_responses()
    
    # Example of accessing the data
    if responses:
        print("\n" + "="*50)
        print("EXAMPLE DATA ACCESS")
        print("="*50)
        print(f"Total responses captured: {len(responses)}")
        
        # You can now parse the GraphQL responses
        for response in responses:
            data = response.get('data')
            # Process your data here based on the structure
            # For example, extract restaurant details, reviews, etc.
            pass

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())