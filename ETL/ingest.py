import datetime
import heapq
import os
from dotenv import load_dotenv
import json

from sqlalchemy.orm import sessionmaker, declarative_base, relationship, backref
from sqlalchemy import create_engine, ForeignKey, Column, Integer, Date, DateTime, String, JSON, Boolean, Float, inspect
from sqlalchemy_utils import database_exists, create_database
from collections import defaultdict, OrderedDict

import statistics

import time

import faulthandler

faulthandler.enable()

load_dotenv()

engine = create_engine(os.getenv('SQL_CONN_STRING'))

# Create database if it does not exist.
if not database_exists(engine.url):
    create_database(engine.url)
else:
    # Connect the database if exists.
    engine.connect()

Session = sessionmaker(bind=engine)
session = Session()


# Special encoding needed to be compatible with emojis which are in some playlist titles:
class Base(object):
    __table_args__ = {
        'mysql_default_charset': 'utf8mb4',
        'mysql_collate': 'utf8mb4_bin',
    }


Base = declarative_base(cls=Base)


# Association/Junction table for the many-to-many relationship between playlists and songs:
#   (See https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#association-object)
class PlaylistTrack(Base):
    __tablename__ = 'playlist_track'
    playlist_mpd_id = Column(Integer, ForeignKey('playlist.playlist_mpd_id', ondelete='CASCADE'), primary_key=True)
    track_id = Column(String(22), ForeignKey('track.track_id'))
    track_pos = Column(Integer, primary_key=True)

    track = relationship('Track', backref=backref('playlist_tracks', cascade="save-update, delete, delete-orphan"))


def calc_avg(playlist_entity, track_lambda):
    return statistics.mean(map(track_lambda, playlist_entity.tracks))


def calc_min(playlist_entity, track_lambda):
    return min(map(track_lambda, playlist_entity.tracks))


def calc_max(playlist_entity, track_lambda):
    return max(map(track_lambda, playlist_entity.tracks))


def calc_std(playlist_entity, track_lambda):
    return statistics.stdev(map(track_lambda, playlist_entity.tracks))


AGGREGATES = {
    'avg': calc_avg,
    'min': calc_min,
    'max': calc_max,
    'std': calc_std
}

FEATURE_NAMES = ['acousticness',
                 'danceability',
                 'duration_ms',
                 'energy',
                 'instrumentalness',
                 'key',
                 'liveness',
                 'loudness',
                 'mode',
                 'speechiness',
                 'tempo',
                 'time_signature',
                 'valence']


def feature_aggregate_attr_name(feature_name, aggregate_name):
    return feature_name + '_' + aggregate_name


class Playlist(Base):
    __tablename__ = 'playlist'

    playlist_mpd_id = Column(Integer, primary_key=True, autoincrement=False, unique=True, nullable=False)
    playlist_name = Column(String(300), nullable=False)
    mpd_generated_at = Column(DateTime, nullable=False)
    modified_at = Column(Date, nullable=False)
    num_tracks = Column(Integer, nullable=False)
    num_artists = Column(Integer, nullable=False)
    num_albums = Column(Integer, nullable=False)
    num_followers = Column(Integer, nullable=False)
    num_edits = Column(Integer, nullable=False)
    is_collaborative = Column(Boolean, nullable=False)
    duration_ms_total = Column(Integer, nullable=False)

    top_genre_1 = Column(String(50))
    top_genre_2 = Column(String(50))
    top_genre_3 = Column(String(50))

    for aggregate_name, aggregate_calc_func in AGGREGATES.items():
        for feature_name in FEATURE_NAMES:
            vars()[feature_aggregate_attr_name(feature_name, aggregate_name)] = Column(Float, default=-1000000)

    tracks = relationship('PlaylistTrack', backref='playlist', cascade="save-update, delete, delete-orphan")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Track(Base):
    __tablename__ = 'track'

    track_id = Column(String(22), primary_key=True, unique=True, nullable=False, index=True)
    track_name = Column(String(300), nullable=False)
    artist_id = Column(String(22), ForeignKey('artist.artist_id'), nullable=False)
    artist_name = Column(String(300), nullable=False)
    album_id = Column(String(22), nullable=False)
    album_name = Column(String(300), nullable=False)

    artist = relationship('Artist', backref='tracks')

    acousticness = Column(Float, nullable=False)
    danceability = Column(Float, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    energy = Column(Float, nullable=False)
    instrumentalness = Column(Float, nullable=False)
    key = Column(Integer, nullable=False)
    liveness = Column(Float, nullable=False)
    loudness = Column(Float, nullable=False)
    mode = Column(Integer, nullable=False)
    speechiness = Column(Float, nullable=False)
    tempo = Column(Float, nullable=False)
    time_signature = Column(Integer, nullable=False)
    valence = Column(Float, nullable=False)


class Artist(Base):
    __tablename__ = 'artist'
    artist_id = Column(String(22), primary_key=True, unique=True, nullable=False, index=True)
    artist_name = Column(String(300), nullable=False)
    genres = Column(JSON, nullable=False)
    followers = Column(Integer, nullable=False)
    popularity = Column(Integer, nullable=False)


# Ensure that the tables are created in the db:
Base.metadata.create_all(engine)

MILLION_PLAYLIST_DATASET_DATA_PATH = os.getenv('MILLION_PLAYLIST_DATASET_DATA_PATH')

sleepCount = 0


def sleeper():
    global sleepCount
    sleepCount += 1
    if sleepCount % 100 == 0:
        sleep_seconds = 0
        print(f'Sleeping for {sleep_seconds}s to not overload the Spotify API...')
        time.sleep(sleep_seconds)

START_SLICE = 104
NUM_OF_SLICES_TO_LOAD = 8
SLICE_SIZE = 1000

batched_playlists = []
batched_track_entities = {}
tracks_to_pull_list = []

# All Track IDs of playlist tracks. Note that these are not unique, and will have duplicates for every time a track is
# in a playlist.
all_playlist_track_ids = []
artist_ids_to_pull = set()

# print('Loading existing database Tracks...')
# # Track entities already in the database:
# existing_track_entities = session.query(Track).all()
# print('Loaded db Tracks.')

loaded_playlist_min_pid = (START_SLICE + NUM_OF_SLICES_TO_LOAD) * SLICE_SIZE
loaded_playlist_max_pid = 0

# Map of Track IDs to respective MPD Track JSON data.
new_tracks_mpd_data = {}

unique_track_ids = set()
unique_artist_ids = set()
track_id_to_playlist_track_entities = defaultdict(set)

total_time_counter_start = time.perf_counter()

load_mpd_time_counter_start = time.perf_counter()

for slice_i in range(START_SLICE, START_SLICE + NUM_OF_SLICES_TO_LOAD):
    print('Loading MPD slice ' + str(slice_i) + '...')
    start_playlist_i = slice_i * SLICE_SIZE
    end_playlist_i = ((slice_i + 1) * SLICE_SIZE) - 1
    slice_file_name = 'mpd.slice.' + str(start_playlist_i) + '-' + str(end_playlist_i) + '.json'
    mpd_slice_data = json.load(open(os.path.join(MILLION_PLAYLIST_DATASET_DATA_PATH, slice_file_name)))
    slice_generated_on = mpd_slice_data['info']['generated_on']
    slice_generated_date_utc = datetime.datetime.fromisoformat(slice_generated_on)
    slice_playlists = mpd_slice_data['playlists']
    for playlist in slice_playlists:
        playlist_pid = playlist['pid']
        # if playlist_pid < 43690 or playlist_pid > 43690:
        #     continue  # TODO REMOVE BREAK - THIS IS JUST FOR SMALLER TESTING DATA.
        if playlist_pid % 100 == 0:
            print(f'Loading playlists in range {playlist_pid} to {playlist_pid + 100}...')
        loaded_playlist_min_pid = min(playlist_pid, loaded_playlist_min_pid)
        loaded_playlist_max_pid = max(playlist_pid, loaded_playlist_max_pid)
        last_modified_epoch_seconds_utc = playlist['modified_at']
        last_modified_date_utc = datetime.datetime.utcfromtimestamp(last_modified_epoch_seconds_utc).date()
        playlist_entity = Playlist(playlist_mpd_id=playlist_pid,
                                   playlist_name=playlist['name'],
                                   mpd_generated_at=slice_generated_date_utc,
                                   modified_at=last_modified_date_utc,
                                   num_tracks=playlist['num_tracks'],
                                   num_artists=playlist['num_artists'],
                                   num_albums=playlist['num_albums'],
                                   num_followers=playlist['num_followers'],
                                   num_edits=playlist['num_edits'],
                                   is_collaborative=(playlist['collaborative'] == 'true'),
                                   duration_ms_total=playlist['duration_ms'])
        batched_playlists.append(playlist_entity)

        # Add Track Entity and Playlist Track objects, but no API data yet:
        for track in playlist['tracks']:
            track_uri = track['track_uri']
            track_id = track_uri[track_uri.rindex(':') + 1:]
            playlist_track_entity = PlaylistTrack(playlist_mpd_id=playlist_pid,
                                                  track_pos=track['pos'])

            playlist_track_entity.playlist_backref = playlist_entity
            unique_track_ids.add(track_id)

            artist_uri = track['artist_uri']
            artist_id = artist_uri[artist_uri.rindex(':') + 1:]
            unique_artist_ids.add(artist_id)

            playlist_entity.tracks.append(playlist_track_entity)
            track_id_to_playlist_track_entities[track_id].add(playlist_track_entity)

            all_playlist_track_ids.append(track_id)

            new_tracks_mpd_data[track_id] = track

load_mpd_time_counter_end = time.perf_counter()

database_track_ids = set()
database_artists = dict()


def add_track_entity(track_entity):
    track_id = track_entity.track_id
    database_track_ids.add(track_id)
    batched_track_entities[track_id] = track_entity
    # track_entity.playlist_track = playlist_track_entity # ???
    for playlist_track_entity in track_id_to_playlist_track_entities[track_id]:
        playlist_track_entity.track = track_entity

unique_tracks_count_stat = len(unique_track_ids)
unique_artists_count_stat = len(unique_artist_ids)

# NOTE: These database fetching queries can be extremely large depending on how many Tracks/Artists are queried for -
# so the max_allowed_packet setting may need to be set in MySQL for example.
print(f"Fetching existing database Tracks (need {unique_tracks_count_stat})...")
fetch_db_tracks_time_counter_start = time.perf_counter()
# Fetch the wanted tracks that are already in the database:
database_tracks = session.query(Track).filter(Track.track_id.in_(unique_track_ids)).all()

print(f"Adding {len(database_tracks)} Tracks from database...")
s = time.perf_counter()

for database_track in database_tracks:
    add_track_entity(database_track)
    del new_tracks_mpd_data[database_track.track_id]

fetch_db_tracks_time_counter_end = time.perf_counter()
print(f'(Took {time.perf_counter() - s}s to add from database.')

# Optimization Note: MUCH faster to just fetch all the artists separately here than to get them off of the fetched
# tracks above. I believe this is because doing so off of the track objects does a single lazy load query for each
# track artist, rather than smartly batching the queries together like we do here:
print(f"Fetching existing database Artists (need {unique_artists_count_stat})...")
fetch_db_artists_time_counter_start = time.perf_counter()
s = time.perf_counter()
database_artists_queried = session.query(Artist).filter(Artist.artist_id.in_(unique_artist_ids)).all()
for database_artist in database_artists_queried:
    database_artists[database_artist.artist_id] = database_track.artist
print(f'(Took {time.perf_counter() - s}s to fetch.')
fetch_db_artists_time_counter_end = time.perf_counter()

print("Loading new Tracks from in-memory MPD data...")
loading_mpd_w_db_time_counter_start = time.perf_counter()
pulled_artist_id_to_track_entities = defaultdict(set)

for track_id, mpd_track_data in new_tracks_mpd_data.items():
    if track_id not in batched_track_entities:
        artist_uri = mpd_track_data['artist_uri']
        artist_id = artist_uri[artist_uri.rindex(':') + 1:]
        album_uri = mpd_track_data['album_uri']
        album_id = album_uri[album_uri.rindex(':') + 1:]
        track_entity = Track(track_id=track_id,
                             track_name=mpd_track_data['track_name'],
                             artist_id=artist_id,
                             artist_name=mpd_track_data['artist_name'],
                             album_id=album_id,
                             album_name=mpd_track_data['album_name'],
                             duration_ms=mpd_track_data['duration_ms'])
        # track_entity.artist_id = artist_id  # We want to ref artist_id internally, but not in the actual schema.
        pulled_artist_id_to_track_entities[artist_id].add(track_entity)
        if artist_id in database_artists:
            track_entity.artist = database_artists[artist_id]
        else:
            artist_ids_to_pull.add(artist_id)
        tracks_to_pull_list.append(track_entity)

        add_track_entity(track_entity)

loading_mpd_w_db_time_counter_end = time.perf_counter()

playlists_count_stat = len(batched_playlists)
playlist_tracks_count_stat = len(all_playlist_track_ids)
tracks_to_pull_count_stat = len(tracks_to_pull_list)
artists_to_pull_count_stat = len(artist_ids_to_pull)
print(f'Loaded {str(NUM_OF_SLICES_TO_LOAD)} MPD slices (PIDs'
      f' {loaded_playlist_min_pid}'
      f'-{loaded_playlist_max_pid}), filling in existing database data. There is a total of '
      f'{playlists_count_stat} playlists, '
      f'{playlist_tracks_count_stat} playlist tracks, '
      f'{unique_tracks_count_stat} unique tracks, '
      f'{tracks_to_pull_count_stat} new tracks to pull, '
      f'and {artists_to_pull_count_stat} artists to pull.')

print(f'\nPulling {len(tracks_to_pull_list)} Tracks from Spotify API...')

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

api_tracks_time_counter_start = time.perf_counter()

# Tracks which need data from the Spotify API:
# tracks_to_pull_list = list(batched_track_entities.values())
MAX_SPOTIFY_TRACKS_PER_REQ = 100
index = 0
while index < len(tracks_to_pull_list):
    track_batch_start_index = index
    track_batch_end_index = min(track_batch_start_index + MAX_SPOTIFY_TRACKS_PER_REQ, len(tracks_to_pull_list))
    index = track_batch_end_index
    print('Pulling Tracks ' + str(track_batch_start_index) + ' to ' +
          str(track_batch_end_index) + ' from Spotify API...')
    track_batch = tracks_to_pull_list[track_batch_start_index:track_batch_end_index]
    track_batch_ids = list(map(lambda track_entity: track_entity.track_id, track_batch))

    # for track_id in track_batch_ids:
    #     database_track = session.query(Track).filter(Track.track_id == track_id).first()
    #     if database_track is None:

    audio_features_response = spotify.audio_features(tracks=track_batch_ids)
    sleeper()

    for j in range(len(track_batch)):
        track_entity = track_batch[j]
        track_audio_features = audio_features_response[j]

        # If track is not found in Spotify, then this probably means it was deleted from Spotify for whatever reason:
        if track_audio_features is None:
            print(f"No audio features found for Track ID: {track_entity.track_id}")
            with open("failed_track_ids.txt", "a+") as no_audio_features_file:
                no_audio_features_file.write(f'{track_entity.track_id}\n')

            print(f"r t {len(track_entity.playlist_tracks)}")
            for playlist_track in track_entity.playlist_tracks:
                p = playlist_track.playlist
                print(f"r {p}")
                print(f"r b {len(playlist_track.playlist_backref.tracks)}")
                playlist_track.playlist_backref.tracks.remove(playlist_track)
                playlist_track.delete_me = True
                print(f"r a {len(playlist_track.playlist_backref.tracks)}")
            track_entity.artist_id = None
            track_entity.artist = None
            track_entity.playlist_tracks.clear()
            track_entity.delete_me = True;
            continue

        # Fill Track Spotify Features:
        for feature_name in FEATURE_NAMES:
            setattr(track_entity, feature_name, track_audio_features[feature_name])

api_tracks_time_counter_end = time.perf_counter()

# def get_track_features(track_entity):
#     return [track_entity.acousticness, track_entity.danceability, track_entity.energy, track_entity.instrumentalness,
#             track_entity.liveness, track_entity.loudness, track_entity.speechiness, track_entity.tempo,
#             track_entity.valence, track_entity.key, track_entity.time_signature]


print('\nPulling ' + str(len(artist_ids_to_pull)) + ' Artists from Spotify API...')
api_artists_time_counter_start = time.perf_counter()

MAX_SPOTIFY_ARTISTS_PER_REQ = 50
artist_ids = list(artist_ids_to_pull)
pulled_artist_genres = dict()
artist_id_index = 0
while artist_id_index < len(artist_ids):
    artist_batch_start_index = artist_id_index
    artist_batch_end_index = min(artist_batch_start_index + MAX_SPOTIFY_ARTISTS_PER_REQ, len(artist_ids))
    artist_id_index = artist_batch_end_index
    print('Pulling Artists ' + str(artist_batch_start_index) + ' to ' +
          str(artist_batch_end_index) + ' from Spotify API...')
    artist_batch = artist_ids[artist_batch_start_index:artist_batch_end_index]
    artists_response = spotify.artists(artists=artist_batch)['artists']
    sleeper()
    for j in range(len(artist_batch)):
        artist_id = artist_batch[j]
        artist_data = artists_response[j]
        pulled_artist_genres[artist_id] = artist_data['genres']

        artist_entity = Artist(artist_id=artist_id,
                               artist_name=artist_data['name'],
                               genres=artist_data['genres'],
                               followers=artist_data['followers']['total'],
                               popularity=artist_data['popularity'])
        for track_entity in pulled_artist_id_to_track_entities[artist_id]:
            track_entity.artist = artist_entity

api_artists_time_counter_end = time.perf_counter()

print('\nFinished pulling from Spotify API.\n')

print('Calculating Playlist Genres and Aggregates...')
calc_playlist_aggregates_time_counter_start = time.perf_counter()

for playlist_entity in batched_playlists:
    if playlist_entity.playlist_mpd_id % 100 == 0:
        print(f'Calculating genres for playlists in range {playlist_entity.playlist_mpd_id} to'
              f' {playlist_entity.playlist_mpd_id + 100}...')
    genre_counts = defaultdict(float)
    # print(playlist_entity.name)
    for playlist_track in playlist_entity.tracks:
        # track_artist_id = playlist_track.track.artist_id
        # if playlist_track.track.artist_genres is None:
        #     track_artist_genres = []
        #     for track_artist_genre in pulled_artist_genres[track_artist_id]:
        #         track_artist_genres.append(track_artist_genre)
        #     playlist_track.track.artist_genres = track_artist_genres

        for track_artist_genre in playlist_track.track.artist.genres:
            genre_counts[track_artist_genre] += 1

        # if hasattr(playlist_track.track, 'artist_ids'):
        # print(playlist_track.track)
        # for artist_id in playlist_track.track.artist_ids:
        #     # print(artist_id)
        #     for genre in artist_genres[artist_id]:
        #         # print(genre)
        #         genre_counts[genre] += 1 / len(playlist_track.track.artist_ids)
        # else:
        #     pass # SHOULD NEVER HAPPEN, AS INVALID TRACKS SHOULD HAVE BEEN REMOVED BEFORE THIS STAGE.

    top_genres = heapq.nlargest(3, genre_counts.items(), key=lambda x: x[1])
    # top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    # TODO REFACTOR:
    if len(top_genres) >= 1:
        playlist_entity.top_genre_1 = top_genres[0][0]
        if len(top_genres) >= 2:
            playlist_entity.top_genre_2 = top_genres[1][0]
            if len(top_genres) >= 3:
                playlist_entity.top_genre_3 = top_genres[2][0]

    for aggregate_name, aggregate_calc_func in AGGREGATES.items():
        for feature_name in FEATURE_NAMES:
            setattr(playlist_entity, feature_aggregate_attr_name(feature_name, aggregate_name),
                    aggregate_calc_func(playlist_entity, lambda playlist_track: getattr(playlist_track.track,
                                                                                        feature_name)))

    # for feature_attr_name in feature_attr_names():
    #     #get_track_feature_lambda = lambda t: getattr(t.track, feature_attr_name)
    #     setattr(playlist_entity, avg_feature_attr_name(feature_attr_name),
    #             playlist_entity.get_avg(get_playlist_track_feature_val))
    #     setattr(playlist_entity, min_feature_attr_name(feature_attr_name),
    #             playlist_entity.get_min(get_playlist_track_feature_val))
    #     setattr(playlist_entity, max_feature_attr_name(feature_attr_name),
    #             playlist_entity.get_max(get_playlist_track_feature_val))
    #     setattr(playlist_entity, std_feature_attr_name(feature_attr_name),
    #             playlist_entity.get_std(get_playlist_track_feature_val))

    # playlist_entity.acousticness = playlist_entity.get_avg(lambda t: t.track.acousticness)
    # playlist_entity.danceability = playlist_entity.get_avg(lambda t: t.track.danceability)
    # playlist_entity.energy = playlist_entity.get_avg(lambda t: t.track.energy)
    # playlist_entity.instrumentalness = playlist_entity.get_avg(lambda t: t.track.instrumentalness)
    # playlist_entity.liveness = playlist_entity.get_avg(lambda t: t.track.liveness)
    # playlist_entity.loudness = playlist_entity.get_avg(lambda t: t.track.loudness)
    # playlist_entity.speechiness = playlist_entity.get_avg(lambda t: t.track.speechiness)
    # playlist_entity.tempo = playlist_entity.get_avg(lambda t: t.track.tempo)
    # playlist_entity.valence = playlist_entity.get_avg(lambda t: t.track.valence)
    # playlist_entity.key = playlist_entity.get_avg(lambda t: t.track.key)
    # playlist_entity.time_signature = playlist_entity.get_avg(lambda t: t.track.time_signature)

calc_playlist_aggregates_time_counter_end = time.perf_counter()

loading_time_counter_start = time.perf_counter()

playlist_id_start_range = loaded_playlist_min_pid
playlist_id_end_range = loaded_playlist_max_pid
print(f'Deleting old database playlists from PID {playlist_id_start_range} to {playlist_id_end_range}...')
# Note: Deleting the playlist cascade deletes the PlaylistTrack associations, but the tracks have to be deleted
# separately since they are one to many with PlaylistTracks.
session.query(Playlist).where(Playlist.playlist_mpd_id.in_(range(playlist_id_start_range, playlist_id_end_range + 1))).delete()

# session.query(Track).where(Track.track_id.in_(all_unique_track_ids)).delete()
session.commit()

# for playlist in batched_playlists:
#     to_delete = []
#     for playlist_track in playlist.tracks:
#         if hasattr(playlist_track, 'delete_me'):
#             to_delete.append(playlist_track)
#
#     print(f"old: {len(playlist.tracks)}")
#     for remove_me in to_delete:
#         playlist.tracks.remove(remove_me)
#     print(f"new: {len(playlist.tracks)}")

print('Copying objects to prevent ghost bugs...')
s = time.perf_counter()

playlist_cols = inspect(Playlist).attrs

# bug fix with ghost playlist track:
isolated_playlists = []
for playlist in batched_playlists:
    isolated_playlist = Playlist(playlist_mpd_id=playlist.playlist_mpd_id, tracks=[])
    playlist_tracks = list(playlist.tracks)
    for playlist_col in playlist_cols:
        setattr(isolated_playlist, playlist_col.key, getattr(playlist, playlist_col.key))
    isolated_playlist.tracks = playlist_tracks.copy()
    isolated_playlists.append(isolated_playlist)

print('Loading to database...')
session.add_all(isolated_playlists)
session.commit()
session.close()


loading_time_counter_end = time.perf_counter()

print(f'Finished loading to database. (Took {time.perf_counter() - s}s).')

print(f'Writing Metrics...')
total_time_counter_end = time.perf_counter()

def timeFormat(timeDelta):
    return format(timeDelta, '.6f')

time_metrics = OrderedDict({
    'total_time': total_time_counter_end - total_time_counter_start,
    'load_mpd_time': load_mpd_time_counter_end - load_mpd_time_counter_start,
    'fetch_db_tracks_time': fetch_db_tracks_time_counter_end - fetch_db_tracks_time_counter_start,
    'fetch_db_artists_time': fetch_db_artists_time_counter_end - fetch_db_artists_time_counter_start,
    'loading_mpd_w_db': loading_mpd_w_db_time_counter_end - loading_mpd_w_db_time_counter_start,
    'api_tracks_time': api_tracks_time_counter_end - api_tracks_time_counter_start,
    'api_artists_time': api_artists_time_counter_end - api_artists_time_counter_start,
    'calculating_playlist_attrs_time': calc_playlist_aggregates_time_counter_end - \
                                       calc_playlist_aggregates_time_counter_start,
    'loading_time': loading_time_counter_end - loading_time_counter_start
})

slice_avg_time_vals = [str(timeFormat(time_metric_time_val / NUM_OF_SLICES_TO_LOAD)) for time_metric_time_val in \
        time_metrics.values()]
slice_time_csv_row = ', '.join(slice_avg_time_vals)

csv_lines_str = ''
for slice_i in range(START_SLICE, START_SLICE + NUM_OF_SLICES_TO_LOAD):
    csv_lines_str += f'{slice_i}, {slice_time_csv_row}\n'

print(f'Appending Metrics Lines:\n{csv_lines_str}')

while True:
    try:
        with open("ingestion_metrics.csv", "a+") as file:
            file.write(csv_lines_str)
    except PermissionError as e:
        print(f'{e} --- Retrying...')
        time.sleep(5)
        continue
    break


batch_metrics_csv_line = f'{START_SLICE}, {NUM_OF_SLICES_TO_LOAD}, ' \
                         f'{playlists_count_stat}, ' \
                         f'{playlist_tracks_count_stat}, ' \
                         f'{unique_tracks_count_stat}, ' \
                         f'{tracks_to_pull_count_stat}, ' \
                         f'{unique_artists_count_stat}, ' \
                         f'{artists_to_pull_count_stat}\n'
print(f'Appending Batch Metrics Line:\n{batch_metrics_csv_line}')

while True:
    try:
        with open("batch_metrics.csv", "a+") as file:
            file.write(batch_metrics_csv_line)
    except PermissionError as e:
        print(f'{e} --- Retrying...')
        time.sleep(5)
        continue
    break

print(f'Finished full ingestion of slices. (Took {time.perf_counter() - total_time_counter_start}s).')
