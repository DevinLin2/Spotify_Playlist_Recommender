
from fastapi import FastAPI, HTTPException
import numpy as np
import json


app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello from ToDo API!"}

@app.get("/square/{num}")
async def get_task(num):
    # Get the task from the table.
    return int(num)**2

@app.get("/find-playlists/{q}")
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