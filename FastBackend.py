"DONT USE"


from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from nlpForSpotify import nl2features

import numpy as np
import json

import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd

import os


# DONT USE


app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = db.create_engine('mysql+pymysql://root:password@localhost:3306/351_proj_db2')
conn = engine.connect()

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello from ToDo API!"}

@app.get("/square/{num}")
async def get_task(num):
    # Get the task from the table.
    return {int(num)**2}

@app.get("/tracks/{limit}")
async def get_tracks(limit):
    """
    Get 'limit' number of tracks from db (test)
    """
    query = "SELECT track_name, track_id FROM track limit 10"

    conn = engine.connect()

    temp = conn.execute(query).fetchall()
    conn.close()
    return {'tracks' : temp}




@app.get("/find-playlists-from-nl/{q}")
async def foo():
    artists = ['Michael Jackson', 'Taylor Swift']

    genres = ['Pop', 'Country']

    acousticness = 1
    danceability = 1
    energy = 0
    instrumentalness = 1
    liveness = -1

    d = dict()
    d['artists'] = artists
    d['genres'] = genres
    d['acousticness'] = acousticness
    d['danceability'] = danceability
    d['energy'] = energy
    d['instrumentalness'] = instrumentalness
    d['liveness'] = liveness

    return d