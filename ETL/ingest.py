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

# metadata = MetaData()
#
# tracks_table = Table(
#     'tracks',
#     metadata,
#     Column('track_name', String(255), unique=False),
#     Column('artist_name', String(255), unique=False),
#     Column('duration_ms', Float()),
#     extend_existing=True
# )
# # Use the metadata to create the table
# metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

class Base(object):
    __table_args__ = {
        "mysql_default_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_bin",
    }

Base = declarative_base(cls=Base)

# Association/Junction table for the many-to-many relationship between playlists and songs:
playlist_track_table = Table(
    'playlist_track',
    Base.metadata,
    Column('playlist_id', Integer, ForeignKey('playlist.id'), primary_key=True),
    Column('track_id', Integer, ForeignKey('track.id'), primary_key=True),
)


class Playlist(Base):
    __tablename__ = 'playlist'

    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(250))
    is_collaborative = Column(Boolean)
    # modified_at = Column(Time)
    num_tracks = Column(Integer)
    num_albums = Column(Integer)
    num_followers = Column(Integer)
    duration_ms = Column(Integer)

    tracks = relationship('Track', secondary=playlist_track_table, backref='playlists')


class Track(Base):
    __tablename__ = 'track'

    id = Column(Integer, primary_key=True)
    track_uri = Column(String(100))
    track_name = Column(String(250))
    artist_name = Column(String(200))
    duration_ms = Column(Integer)


# Ensure that the tables are created in the db:
Base.metadata.create_all(engine)

# track_test = Track(track_name='test_name', artist_name='test_artist', duration_ms='1')


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
            if False:
            #if track_uri in batched_track_entities:
                track_entity = batched_track_entities[track_uri]
            else:
                track_entity = Track(track_uri=track_uri, track_name=track['track_name'],
                                     artist_name=track['artist_name'], duration_ms=track['duration_ms'])
                batched_track_entities[track_uri] = track_entity
            playlist_entity.tracks.append(track_entity)

# batched_tracks2 = [track for playlist in batched_playlists for track in playlist['tracks']]

session.add_all(batched_playlists)

# session.add_all(batched_track_entities.values())

session.commit()
