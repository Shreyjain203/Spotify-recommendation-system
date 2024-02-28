# !pip install spotipy
import spotipy
from spotipy.oauth2 import SpotifyOAuth

def auth():
    # Get the authentication credentials from user
    client_id = ''
    client_secret = ''
    redirect_uri = ''

    # Set up the authentication credentials
    sp = spotipy.Spotify(auth_manager = SpotifyOAuth(client_id = client_id,
                                                client_secret = client_secret, 
                                                redirect_uri = redirect_uri,
                                                scope = "user-library-read user-read-recently-played"))
    return sp