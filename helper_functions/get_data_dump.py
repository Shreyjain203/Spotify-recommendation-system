# !pip install spotipy

# Spotify Data Retrieval Script (get the top playlist for a particular category and get track details for that)
from download_data import get_data
from spotify_authentication import auth

def get_data_dump():
    sp = auth()

    # Getting the list of all the categories
    categories_list = {i['name']:i['id'] for i in sp.categories('in')['categories']['items']}

    categories_ids = [categories_list['Hindi'],
                    categories_list['Indie'],
                    categories_list['Punjabi']]

    # Getting top playlist from list of all the playlist ids in all those categories
    playlist_names = {}
    playlist_ids = []
    for categories_id in categories_ids:
        data = sp.category_playlists(categories_id)
        playlist_names[categories_id] = [{i['name']:i['tracks']['total']} for i in data['playlists']['items']]
        playlist_id = [data['playlists']['items'][0]['id']]
        playlist_ids += playlist_id

    playlist_ids = list(set(playlist_ids))

    song_meta_data = []
    for playlist_id in playlist_ids:
        playlist_data = sp.playlist_tracks(playlist_id)["items"]
        song_meta_data = get_data(sp, playlist_data, song_meta_data = song_meta_data)

    return song_meta_data