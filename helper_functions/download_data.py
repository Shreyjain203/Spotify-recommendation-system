# !pip install spotipy

def get_data(sp, recently_played, song_meta_data = []):
    track_ids = list(set([track['track']['id'] for track in recently_played]))

    # In one batch this api key can only extract upto 100 song's audio features
    audio_features = []
    for i in range(0,len(track_ids)+2, 100):
        new_batch = sp.audio_features(track_ids[i:i+100])
        if new_batch[0]:
            audio_features += new_batch

    for track in recently_played:
        track_id = track['track']['id']
        audio_analysis = sp.audio_analysis(track_id)
        if track['track']['name'] not in song_meta_data:
            document = {track['track']['name']: {'release_date': track['track']['album']['release_date'],
                                                    'song_id': track_id,
                                                    'artist_name': track['track']['artists'][0]['name'],
                                                    'album_name': track['track']['album']['name'],
                                                    'available_markets': track['track']['album']['available_markets'],
                                                    'audio_features': audio_features[track_ids.index(track_id)],
                                                    'beats': audio_analysis['beats'],
                                                    'bars': audio_analysis['bars'],
                                                    'rhythm_string': audio_analysis['track']['rhythmstring'],
                                                    'segments': audio_analysis['segments']}}
            song_meta_data.append(document)
    return song_meta_data
