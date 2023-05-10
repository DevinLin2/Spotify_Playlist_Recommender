# import copy
import datetime
import os
from dotenv import load_dotenv
import json

from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import create_engine, ForeignKey, Column, Integer, Date, DateTime, String, JSON, Boolean, Float
from sqlalchemy_utils import database_exists, create_database

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
    playlist_mpd_id = Column(Integer, ForeignKey('playlist.mpd_id', ondelete='CASCADE'), primary_key=True)
    track_row_id = Column(Integer, ForeignKey('track.row_id'), primary_key=True)
    track_pos = Column(Integer, primary_key=True)

    track = relationship('Track', backref='playlisttrack')


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

    mpd_id = Column(Integer, primary_key=True, autoincrement=False, unique=True, nullable=False)
    name = Column(String(250), nullable=False)
    mpd_generated_at = Column(DateTime)
    modified_at = Column(Date)
    num_tracks = Column(Integer)
    num_artists = Column(Integer)
    num_albums = Column(Integer)
    num_followers = Column(Integer)
    num_edits = Column(Integer)
    is_collaborative = Column(Boolean)
    duration_ms_total = Column(Integer)

    top_genre_1 = Column(String(50))
    top_genre_2 = Column(String(50))
    top_genre_3 = Column(String(50))

    for aggregate_name, aggregate_calc_func in AGGREGATES.items():
        for feature_name in FEATURE_NAMES:
            vars()[feature_aggregate_attr_name(feature_name, aggregate_name)] = Column(Float)

    tracks = relationship('PlaylistTrack', backref='playlist')


class Track(Base):
    __tablename__ = 'track'

    row_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False)
    track_id = Column(String(22), primary_key=True, unique=True, nullable=False, index=True)
    track_name = Column(String(250), nullable=False)
    artist_id = Column(String(22), nullable=False)
    artist_name = Column(String(200), nullable=False)
    artist_genres = Column(JSON)
    album_id = Column(String(22), nullable=False)
    album_name = Column(String(200), nullable=False)

    acousticness = Column(Float)
    danceability = Column(Float)
    duration_ms = Column(Integer)
    energy = Column(Float)
    instrumentalness = Column(Float)
    key = Column(Integer)
    liveness = Column(Float)
    loudness = Column(Float)
    mode = Column(Integer)
    speechiness = Column(Float)
    tempo = Column(Float)
    time_signature = Column(Integer)
    valence = Column(Float)


# Ensure that the tables are created in the db:
Base.metadata.create_all(engine)

MILLION_PLAYLIST_DATASET_DATA_PATH = os.getenv('MILLION_PLAYLIST_DATASET_DATA_PATH')

sleepCount = 0


def sleeper():
    global sleepCount
    sleepCount += 1
    if sleepCount % 100 == 0:
        sleep_seconds = 1
        print(f'Sleeping for {sleep_seconds}s to not overload the Spotify API...')
        time.sleep(sleep_seconds)


START_SLICE = 0
NUM_OF_SLICES_TO_LOAD = 1
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
        # if playlist_pid > 1004:
        #     break  # TODO REMOVE BREAK - THIS IS JUST FOR SMALLER TESTING DATA.
        if (playlist_pid % 100 == 0):
            print(f'Loading playlists in range {playlist_pid} to {playlist_pid + 100}...')
        loaded_playlist_min_pid = min(playlist_pid, loaded_playlist_min_pid)
        loaded_playlist_max_pid = max(playlist_pid, loaded_playlist_max_pid)
        last_modified_epoch_seconds_utc = playlist['modified_at']
        last_modified_date_utc = datetime.datetime.utcfromtimestamp(last_modified_epoch_seconds_utc).date()
        playlist_entity = Playlist(mpd_id=playlist_pid,
                                   name=playlist['name'],
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
            track_entity = None
            track_id = track_uri[track_uri.rindex(':') + 1:]
            if track_id in batched_track_entities:
                track_entity = batched_track_entities[track_id]
                track_entity.playlist_tracks_backref.append(playlist_entity.tracks)
            else:
                # print(f'Trying to load track from database: {track_id}...')
                database_track = session.query(Track).filter(Track.track_id == track_id).first()
                if database_track is None:
                    artist_uri = track['artist_uri']
                    artist_id = artist_uri[artist_uri.rindex(':') + 1:]
                    album_uri = track['album_uri']
                    album_id = album_uri[album_uri.rindex(':') + 1:]
                    track_entity = Track(track_id=track_id,
                                         track_name=track['track_name'],
                                         artist_id=artist_id,
                                         artist_name=track['artist_name'],
                                         album_id=album_id,
                                         album_name=track['album_name'],
                                         duration_ms=track['duration_ms'])
                    artist_ids_to_pull.add(artist_id)
                    tracks_to_pull_list.append(track_entity)
                else:
                    track_entity = database_track
                    # track_entity = copy.deepcopy(database_track)
                    # track_entity = Track(track_id=track_id,
                    #                      track_name=track['track_name'],
                    #                      artist_id=database_track.artist_id,
                    #                      artist_name=track['artist_name'],
                    #                      album_id=database_track.album_id,
                    #                      album_name=track['album_name'],
                    #                      duration_ms=track['duration_ms'])
                    # for feature_name in FEATURE_NAMES:
                    #     setattr(track_entity, feature_name, getattr(database_track, feature_name))

                track_entity.playlist_tracks_backref = [playlist_entity.tracks]
                batched_track_entities[track_id] = track_entity

            all_playlist_track_ids.append(track_id)
            playlist_track_link = PlaylistTrack(track_pos=track['pos'])
            track_entity.playlist_track = playlist_track_link
            playlist_track_link.track = track_entity
            playlist_entity.tracks.append(playlist_track_link)

all_unique_track_ids = list(batched_track_entities.keys())

print(f'Finished loading {str(NUM_OF_SLICES_TO_LOAD)} slices for a total of '
      f'{str(len(batched_playlists))} playlists, '
      f'{str(len(all_playlist_track_ids))} playlist tracks, '
      f'{str(len(all_unique_track_ids))} unique tracks, '
      f'and {str(len(tracks_to_pull_list))} new tracks to pull.')

print(f'\nPulling {len(tracks_to_pull_list)} Tracks from Spotify API...')

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

# Tracks which need data from the Spotify API:
# tracks_to_pull_list = list(batched_track_entities.values())
MAX_SPOTIFY_TRACKS_PER_REQ = 100
index = 0
while index < len(tracks_to_pull_list):
    track_batch_start_index = index
    track_batch_end_index = min(track_batch_start_index + MAX_SPOTIFY_TRACKS_PER_REQ, len(tracks_to_pull_list))
    index = track_batch_end_index
    print('Pulling tracks ' + str(track_batch_start_index) + ' to ' +
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
            for playlist_tracks in track_entity.playlist_tracks_backref:
                if track_entity.playlist_track in playlist_tracks:
                    playlist_tracks.remove(track_entity.playlist_track)
            continue

        # Fill Track Spotify Features:
        for feature_name in FEATURE_NAMES:
            setattr(track_entity, feature_name, track_audio_features[feature_name])

# def get_track_features(track_entity):
#     return [track_entity.acousticness, track_entity.danceability, track_entity.energy, track_entity.instrumentalness,
#             track_entity.liveness, track_entity.loudness, track_entity.speechiness, track_entity.tempo,
#             track_entity.valence, track_entity.key, track_entity.time_signature]


print('\nPulling ' + str(len(artist_ids_to_pull)) + ' Artists from Spotify API...')

from collections import defaultdict

MAX_SPOTIFY_ARTISTS_PER_REQ = 50
artist_ids = list(artist_ids_to_pull)
pulled_artist_genres = dict()
artist_id_index = 0
while artist_id_index < len(artist_ids):
    artist_batch_start_index = artist_id_index
    artist_batch_end_index = min(artist_batch_start_index + MAX_SPOTIFY_ARTISTS_PER_REQ, len(artist_ids))
    artist_id_index = artist_batch_end_index
    print('Pulling artists ' + str(artist_batch_start_index) + ' to ' +
          str(artist_batch_end_index) + ' from Spotify API...')
    artist_batch = artist_ids[artist_batch_start_index:artist_batch_end_index]
    artists_response = spotify.artists(artists=artist_batch)['artists']
    sleeper()
    for j in range(len(artist_batch)):
        artist_id = artist_batch[j]
        artist_data = artists_response[j]
        pulled_artist_genres[artist_id] = artist_data['genres']

print('\nFinished pulling from Spotify API.\n')

print('Calculating Playlist Genres...')

for playlist_entity in batched_playlists:
    if (playlist_entity.mpd_id % 100 == 0):
        print(f'Calculating genres for playlists in range {playlist_entity.mpd_id} to'
              f' {playlist_entity.mpd_id + 100}...')
    genre_counts = defaultdict(float)
    # print(playlist_entity.name)
    for playlist_track in playlist_entity.tracks:
        track_artist_id = playlist_track.track.artist_id
        if playlist_track.track.artist_genres is None:
            track_artist_genres = []
            for track_artist_genre in pulled_artist_genres[track_artist_id]:
                track_artist_genres.append(track_artist_genre)
            playlist_track.track.artist_genres = track_artist_genres

        for track_artist_genre in playlist_track.track.artist_genres:
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

    top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:3]
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

playlist_id_start_range = loaded_playlist_min_pid
playlist_id_end_range = loaded_playlist_max_pid
print(f'Deleting old database playlists from PID {playlist_id_start_range} to {playlist_id_end_range}...')
# Note: Deleting the playlist cascade deletes the PlaylistTrack associations, but the tracks have to be deleted
# separately since they are one to many with PlaylistTracks.
session.query(Playlist).where(Playlist.mpd_id.in_(range(playlist_id_start_range, playlist_id_end_range + 1))).delete()

# session.query(Track).where(Track.track_id.in_(all_unique_track_ids)).delete()
session.commit()

print('Loading to database...')

session.add_all(batched_playlists)
session.commit()

print('Finished loading to database.')
