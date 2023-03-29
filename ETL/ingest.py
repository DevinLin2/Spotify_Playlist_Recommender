import os
from dotenv import load_dotenv
import json

from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import create_engine, ForeignKey, Table, Column, Integer, String, Boolean
from sqlalchemy_utils import database_exists, create_database

engine = create_engine('mysql+pymysql://root:@localhost:6603/351_proj_db?charset=utf8mb4', connect_args={'charset':'utf8mb4'})

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
    playlist_row_id = Column(Integer, ForeignKey('playlist.id'), primary_key=True)
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
    duration_ms = Column(Integer)


# Ensure that the tables are created in the db:
Base.metadata.create_all(engine)

load_dotenv()

MILLION_PLAYLIST_DATASET_DATA_PATH = os.getenv('MILLION_PLAYLIST_DATASET_DATA_PATH')

NUM_OF_SLICES_TO_LOAD = 1
SLICE_SIZE = 1000

batched_playlists = []
batched_track_entities = {}

for slice_i in range(NUM_OF_SLICES_TO_LOAD):
    start_playlist_i = slice_i * SLICE_SIZE
    end_playlist_i = ((slice_i + 1) * SLICE_SIZE) - 1
    slice_file_name = 'mpd.slice.' + str(start_playlist_i) + '-' + str(end_playlist_i) + '.json'
    mpd_slice_data = json.load(open(os.path.join(MILLION_PLAYLIST_DATASET_DATA_PATH, slice_file_name)))
    slice_playlists = mpd_slice_data['playlists']
    for playlist in slice_playlists:
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

            a.track = track_entity
            playlist_entity.tracks.append(a)

session.add_all(batched_playlists)

session.commit()
