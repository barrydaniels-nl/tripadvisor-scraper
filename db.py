import sqlite3
import os
from typing import Optional, List, Dict, Tuple

DATABASE_FILE = "city_restaurant_links.db"


def init_database():
    """Initialize the database and create tables if they don't exist."""
    # Check if database file exists
    db_exists = os.path.exists(DATABASE_FILE)

    if not db_exists:
        print(f"Database file '{DATABASE_FILE}' does not exist. Creating...")

    # Connect to database (creates file if it doesn't exist)
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Enable WAL mode for better concurrent access
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=30000")  # 30 second timeout

    # Create city_restaurant_links table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS city_restaurant_links (
            geoname_id INTEGER,
            url TEXT UNIQUE,
            status TEXT
        )
    """
    )

    conn.commit()
    conn.close()

    if not db_exists:
        print(f"Database '{DATABASE_FILE}' created successfully.")
    print("Database initialized successfully.")


def add_city_restaurant(
    geoname_id: int, url: str, status: str = "pending"
) -> bool:
    """
    Add a new city restaurant record to the database.

    Args:
        geoname_id: The GeoName ID of the city
        url: The URL of the restaurant listing
        status: The status of the record (default: "pending")

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO city_restaurant_links (geoname_id, url, status)
            VALUES (?, ?, ?)
        """,
            (geoname_id, url, status),
        )

        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        print(f"Record with URL {url} already exists.")
        return False
    except Exception as e:
        print(f"Error adding record: {e}")
        return False


def update_city_restaurant(
    url: str, geoname_id: Optional[int] = None, status: Optional[str] = None
) -> bool:
    """
    Update an existing city restaurant record in the database by URL.

    Args:
        url: The URL to identify the record
        geoname_id: The new GeoName ID (optional)
        status: The new status (optional)

    Returns:
        bool: True if successful, False otherwise
    """
    if geoname_id is None and status is None:
        print("Nothing to update. Provide at least geoname_id or status.")
        return False

    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        # Build update query dynamically based on provided parameters
        update_fields = []
        params = []

        if geoname_id is not None:
            update_fields.append("geoname_id = ?")
            params.append(geoname_id)

        if status is not None:
            update_fields.append("status = ?")
            params.append(status)

        params.append(url)

        query = f"""
            UPDATE city_restaurant_links
            SET {', '.join(update_fields)}
            WHERE url = ?
        """

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            print(f"No record found with URL {url}")
            conn.close()
            return False

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error updating record: {e}")
        return False


def get_city_restaurant_urls(geoname_ids: List[int]) -> Dict[int, List[str]]:
    """
    Get restaurant URLs from the database for specified geoname_ids.
    
    Args:
        geoname_ids: List of geoname IDs to retrieve URLs for
        
    Returns:
        dict: Dictionary with geoname_id as key and list of URLs as value
        Example: {123: ['url1', 'url2'], 456: ['url3', 'url4']}
    """
    if not geoname_ids:
        return {}
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Create placeholders for the IN clause
        placeholders = ','.join('?' * len(geoname_ids))
        
        cursor.execute(
            f"""
            SELECT geoname_id, url 
            FROM city_restaurant_links 
            WHERE geoname_id IN ({placeholders})
            ORDER BY geoname_id, url
            """,
            geoname_ids
        )
        
        rows = cursor.fetchall()
        conn.close()
        
        # Group URLs by geoname_id
        result = {}
        for geoname_id, url in rows:
            if geoname_id not in result:
                result[geoname_id] = []
            result[geoname_id].append(url)
        
        return result
        
    except Exception as e:
        print(f"Error retrieving restaurant URLs: {e}")
        return {}


def get_city_restaurant_urls_with_status(
    geoname_ids: List[int], status: Optional[str] = None
) -> Dict[int, List[Tuple[str, str]]]:
    """
    Get restaurant URLs from the database for specified geoname_ids with optional status filter.
    
    Args:
        geoname_ids: List of geoname IDs to retrieve URLs for
        status: Optional status filter ('pending', 'completed', etc.)
        
    Returns:
        dict: Dictionary with geoname_id as key and list of tuples (url, status) as value
        Example: {123: [('url1', 'pending'), ('url2', 'completed')]}
    """
    if not geoname_ids:
        return {}
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Create placeholders for the IN clause
        placeholders = ','.join('?' * len(geoname_ids))
        
        if status:
            query = f"""
                SELECT geoname_id, url, status 
                FROM city_restaurant_links 
                WHERE geoname_id IN ({placeholders}) AND status = ?
                ORDER BY geoname_id, url
            """
            params = geoname_ids + [status]
        else:
            query = f"""
                SELECT geoname_id, url, status 
                FROM city_restaurant_links 
                WHERE geoname_id IN ({placeholders})
                ORDER BY geoname_id, url
            """
            params = geoname_ids
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        # Group URLs by geoname_id
        result = {}
        for geoname_id, url, url_status in rows:
            if geoname_id not in result:
                result[geoname_id] = []
            result[geoname_id].append((url, url_status))
        
        return result
        
    except Exception as e:
        print(f"Error retrieving restaurant URLs with status: {e}")
        return {}


def remove_city_restaurant_url(url: str) -> bool:
    """
    Remove a URL from the database after successful scraping.
    
    Args:
        url: The URL to remove from the database
        
    Returns:
        bool: True if successfully removed, False otherwise
    """
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute(
            "DELETE FROM city_restaurant_links WHERE url = ?",
            (url,)
        )
        
        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        return rows_affected > 0
        
    except Exception as e:
        print(f"Error removing URL from database: {e}")
        return False


if __name__ == "__main__":
    # Initialize the database
    init_database()

    # Example usage
    print("\nAdding sample data...")
    add_city_restaurant(
        5128581,
        "https://www.tripadvisor.com/Restaurants-g60763-New_York_City.html",
        "pending",
    )
    add_city_restaurant(
        2643743,
        "https://www.tripadvisor.com/Restaurants-g186338-London.html",
        "completed",
    )

    print("\nUpdating status for New York...")
    update_city_restaurant(
        "https://www.tripadvisor.com/Restaurants-g60763-New_York_City.html",
        status="in_progress"
    )

    # Verify the data
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM city_restaurant_links")
    rows = cursor.fetchall()

    print("\nCurrent records in database:")
    for row in rows:
        print(f"GeoName ID: {row[0]}, URL: {row[1]}, Status: {row[2]}")

    conn.close()
    
    # Test the new functions
    print("\nTesting get_city_restaurant_urls function...")
    test_geoname_ids = [5128581, 2643743]
    urls_dict = get_city_restaurant_urls(test_geoname_ids)
    for geoname_id, urls in urls_dict.items():
        print(f"GeoName ID {geoname_id}: {len(urls)} URLs")
        for url in urls[:2]:  # Show first 2 URLs
            print(f"  - {url}")
    
    print("\nTesting get_city_restaurant_urls_with_status function...")
    urls_with_status = get_city_restaurant_urls_with_status(test_geoname_ids, "pending")
    for geoname_id, url_status_list in urls_with_status.items():
        print(f"GeoName ID {geoname_id}: {len(url_status_list)} pending URLs")
        for url, status in url_status_list[:2]:  # Show first 2 URLs
            print(f"  - {url} ({status})")
