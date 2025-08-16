import sqlite3
import os
from typing import Optional

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
