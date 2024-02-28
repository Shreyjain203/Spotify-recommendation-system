# !pip install spotipy

# Spotify Data Retrieval Script (prints the user's saved tracks, playlists & recently played tracks)
from download_data import get_data
from spotify_authentication import auth

def get_user_data():
    # Set up the authentication
    sp = auth()

    # Get the user's recently played tracks
    recently_played = []
    results = sp.current_user_recently_played()
    recently_played.extend(results['items'])
    while results['next']:
        results = sp.next(results)
        recently_played.extend(results['items'])

    song_meta_data = get_data(sp, recently_played, song_meta_data = [])
    return song_meta_data
