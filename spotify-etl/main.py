import requests
import pandas as pd
from database.connector import create_connection

# Generate your credentials here : https://developer.spotify.com/console/get-current-user-top-artists-and-tracks/?type=artists
# Note: You need to have a spotify account

API_URL = "https://api.spotify.com/v1"
TOKEN = ""
MYSQL_USER = ""
MYSQL_PASS = ""
MYSQL_DB = ""


class SpotifyExtract:
    def __init__(self, token) -> None:
        self.token = token

    def _generate_headers(self):
        return {"Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer {token}".format(token=TOKEN)}

    def extract_user_top_artists_or_tracks(self, type="artists", time_range="short_term", limit=5):
        endpoint_url = "{url}/me/top/{type}?time_range={time_range}&limit={limit}".format(
            url=API_URL, type=type, time_range=time_range, limit=limit)

        response = requests.get(endpoint_url, headers=self._generate_headers())

        return response.json()

    def check_if_valid_data(self, data_frame: pd.DataFrame) -> bool:
        # Check if dataframe is empty
        if data_frame.empty:
            print("No songs downloaded. Finishing execution")
            return False

        return True


if __name__ == "__main__":
    extractor = SpotifyExtract(token=TOKEN)

    # Download 50 top artsits you've listened to last 4 weeks
    data = extractor.extract_user_top_artists_or_tracks(limit=50)

    names = []
    genres = []
    popularity = []
    followers = []

    # Extract only relevant variables of data from json object
    for artist in data["items"]:
        names.append(artist["name"])
        genres.append(artist["genres"])
        popularity.append(artist["popularity"])
        followers.append(artist["followers"]["total"])

    # Insert into dictionary before turning it into data frame
    artist_dict = {
        "artist_name": names,
        "genres": genres,
        "popularity": popularity,
        "followers_amount": followers
    }

    artist_df = pd.DataFrame(artist_dict, columns=[
                             "artist_name", "genres", "popularity", "followers_amount"])

    # Normalize by exploding genre multiple elements
    artist_df = artist_df.explode("genres")\
        .reset_index(drop=True)

    # Impute data if missing
    artist_df[["artist_name", "genres"]] = artist_df[[
        "artist_name", "genres"]].fillna(value="Undefinded")
    artist_df[["popularity", "followers_amount"]] = artist_df[[
        "popularity", "followers_amount"]].fillna(value=0)

    # Validate
    if extractor.check_if_valid_data(artist_df):
        print("Data valid, proceed to Lead stage")

    # Load
    connection = create_connection(
        host='localhost', database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS)
    cursor = connection.cursor()

    sql_query = \
        """
        CREATE TABLE IF NOT EXISTS my_top_artists(
            my_top_artist INT NOT NULL AUTO_INCREMENT,
            artist_name VARCHAR(255),
            artist_genre VARCHAR(255),
            artist_popularity INT,
            artist_followers_amount INT,
            PRIMARY KEY (my_top_artist)
        )
        """

    cursor.execute(sql_query)
    print("Opened database successfully")

    sql_insert = \
        """INSERT INTO 
                my_top_artists(artist_name, artist_genre, artist_popularity, artist_followers_amount)
           VALUES 
                ( %s, %s, %s, %s)"""

    values = artist_df.to_records(index=False)\
        .tolist()

    cursor.executemany(sql_insert, values)
    connection.commit()

    print("Data loaded successfully")

    connection.close()
