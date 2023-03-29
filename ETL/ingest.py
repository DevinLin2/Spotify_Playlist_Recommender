import os
from dotenv import load_dotenv
import json

from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import create_engine, ForeignKey, Table, Column, Integer, String, Boolean, Float
from sqlalchemy_utils import database_exists, create_database

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
    playlist_row_id = Column(Integer, ForeignKey('playlist.id', ondelete='CASCADE'), primary_key=True)
    track_row_id = Column(Integer, ForeignKey('track.id'), primary_key=True)
    track_pos = Column(Integer, primary_key=True)

    track = relationship('Track', backref='playlisttrack')


class Playlist(Base):
    __tablename__ = 'playlist'

    id = Column(Integer, primary_key=True, autoincrement=False, unique=True, nullable=False)
    name = Column(String(250), nullable=False)
    is_collaborative = Column(Boolean)
    # modified_at = Column(Time)
    num_tracks = Column(Integer)
    num_albums = Column(Integer)
    num_followers = Column(Integer)
    duration_ms = Column(Integer)

    tracks = relationship('PlaylistTrack', backref='playlist')


class Track(Base):
    __tablename__ = 'track'

    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    track_id = Column(String(100), unique=True, nullable=False, index=True)
    track_name = Column(String(250), nullable=False)
    artist_name = Column(String(200), nullable=False)

    acousticness = Column(Float)
    danceability = Column(Float)
    energy = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)
    loudness = Column(Float)
    speechiness = Column(Float)
    tempo = Column(Float)
    valence = Column(Float)

    key = Column(Integer)
    time_signature = Column(String(10))

    duration_ms = Column(Integer)


# Ensure that the tables are created in the db:
Base.metadata.create_all(engine)

MILLION_PLAYLIST_DATASET_DATA_PATH = os.getenv('MILLION_PLAYLIST_DATASET_DATA_PATH')

START_SLICE = 0
NUM_OF_SLICES_TO_LOAD = 1
SLICE_SIZE = 1000

batched_playlists = []
batched_track_entities = {}
all_track_ids = [];

for slice_i in range(START_SLICE, NUM_OF_SLICES_TO_LOAD):
    print('Extracting slice ' + str(slice_i) + '...')
    start_playlist_i = slice_i * SLICE_SIZE
    end_playlist_i = ((slice_i + 1) * SLICE_SIZE) - 1
    slice_file_name = 'mpd.slice.' + str(start_playlist_i) + '-' + str(end_playlist_i) + '.json'
    mpd_slice_data = json.load(open(os.path.join(MILLION_PLAYLIST_DATASET_DATA_PATH, slice_file_name)))
    slice_playlists = mpd_slice_data['playlists']
    for playlist in slice_playlists:
        if playlist['pid'] > 100:
            break  # TODO REMOVE - THIS IS JUST FOR SMALLER TESTING DATA.
        playlist_entity = Playlist(id=playlist['pid'], name=playlist['name'],
                                   is_collaborative=bool(playlist['collaborative']),
                                   num_tracks=playlist['num_tracks'],
                                   num_albums=playlist['num_albums'],
                                   num_followers=playlist['num_followers'],
                                   duration_ms=playlist['duration_ms'])
        batched_playlists.append(playlist_entity)

        for track in playlist['tracks']:
            track_uri = track['track_uri']
            track_entity = None
            a = PlaylistTrack(track_pos=track['pos'])
            if track_uri in batched_track_entities:
                track_entity = batched_track_entities[track_uri]
            else:
                track_id = track_uri[track_uri.rindex(':')+1:]
                track_entity = Track(track_id=track_id, track_name=track['track_name'],
                                     artist_name=track['artist_name'], duration_ms=track['duration_ms'])
                batched_track_entities[track_uri] = track_entity
                all_track_ids.append(track_id)
            a.track = track_entity
            playlist_entity.tracks.append(a)

print('Finished extracting ' + str(NUM_OF_SLICES_TO_LOAD) + ' slices.')

print('Extracting from Spotify API...')

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

tracks_list = list(batched_track_entities.values())
MAX_SPOTIFY_TRACKS_PER_REQ = 100
i = 0
while i < len(tracks_list):
    track_batch_start_index = i
    track_batch_end_index = min(track_batch_start_index+MAX_SPOTIFY_TRACKS_PER_REQ, len(tracks_list))
    i = track_batch_end_index
    print('Extracting tracks ' + str(track_batch_start_index) + ' to ' +
          str(track_batch_end_index) + ' from Spotify API...')
    track_batch = tracks_list[track_batch_start_index:track_batch_end_index]
    track_batch_ids = map(lambda track_entity: track_entity.track_id, track_batch)
    response = spotify.audio_features(tracks=track_batch_ids)
    for j in range(len(track_batch)):
        track_entity = track_batch[j]
        track_api_data = response[j]
        track_entity.acousticness = track_api_data['acousticness']
        track_entity.danceability = track_api_data['danceability']
        track_entity.energy = track_api_data['energy']
        track_entity.instrumentalness = track_api_data['instrumentalness']
        track_entity.liveness = track_api_data['liveness']
        track_entity.loudness = track_api_data['loudness']
        track_entity.speechiness = track_api_data['speechiness']
        track_entity.tempo = track_api_data['tempo']
        track_entity.valence = track_api_data['valence']
        track_entity.key = track_api_data['key']
        track_entity.time_signature = track_api_data['time_signature']




print('Finished extracting from Spotify API.')

print('Deleting old database data...')

# Note: Deleting the playlist cascade deletes the PlaylistTrack associations, but the tracks have to be deleted
# separately since they are one to many with PlaylistTracks.
playlist_id_start_range = SLICE_SIZE * START_SLICE
playlist_id_end_range = (SLICE_SIZE * (START_SLICE + NUM_OF_SLICES_TO_LOAD))
session.query(Playlist).where(Playlist.id.in_(range(playlist_id_start_range, playlist_id_end_range))).delete()

session.query(Track).where(Track.track_id.in_(all_track_ids)).delete()
session.commit()

print('Loading to database...')

session.add_all(batched_playlists)

session.commit()

print('Finished loading to database.')
