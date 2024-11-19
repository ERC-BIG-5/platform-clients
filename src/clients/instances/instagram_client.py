from instaloader import Instaloader, TopSearchResults
from geopy.distance import geodesic
import json

USERNAME = ""
PASSWORD = ""
#SESSION_FILE_NAME = "session.json"

def find_nearby_locations(loader, lat, lng, radius_km=1.0):
    """Search locations near given coordinates."""
    # Search for locations near the point
    locations = []
    try:
        # Use TopSearchResults to search for locations
        search_results = TopSearchResults(loader.context, f"places near {lat},{lng}")

        for loc in search_results.get_locations():
            if loc.lat is None or loc.lng is None:
                continue

            # Calculate distance between points
            distance = geodesic((lat, lng), (loc.lat, loc.lng)).kilometers

            if distance <= radius_km:
                locations.append({
                    'id': loc.userid,
                    'name': loc.name,
                    'lat': loc.lat,
                    'lng': loc.lng,
                    'distance': round(distance, 3)
                })

    except Exception as e:
        print(f"Error searching locations: {e}")

    return locations


def download_location_posts(loader, location_ids, max_posts=50):
    """Download posts from specified locations."""
    for loc_id in location_ids:
        try:
            # Get posts for location
            posts = loader.get_location_posts(loc_id)

            count = 0
            for post in posts:
                if count >= max_posts:
                    break

                try:
                    # Download post
                    loader.download_post(post, target=f"location_{loc_id}")
                    count += 1
                except Exception as e:
                    print(f"Error downloading post: {e}")
                    continue

            print(f"Downloaded {count} posts from location {loc_id}")

        except Exception as e:
            print(f"Error getting posts for location {loc_id}: {e}")
            continue


def main():

    # Login if needed
    # L.login("username", "password")

    # Example coordinates (San Francisco)
    lat, lng = 37.7749, -122.4194
    radius = 1.0  # km

    # Find nearby locations
    print(f"Searching locations within {radius}km of {lat},{lng}")
    locations = find_nearby_locations(L, lat, lng, radius)

    # Save locations to file
    with open('nearby_locations.json', 'w') as f:
        json.dump(locations, f, indent=2)

    print(f"Found {len(locations)} nearby locations")

    # Download posts from each location
    location_ids = [loc['id'] for loc in locations]
    download_location_posts(L, location_ids, max_posts=20)


if __name__ == "__main__":
    #main()
    # Initialize instaloader
    L = Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=True,
        download_comments=False,
        save_metadata=True
    )

    """
    L.load_session_from_file(USERNAME)
    L.login(USERNAME, PASSWORD)
    L.save_session_to_file()

    L.test_login()
    """