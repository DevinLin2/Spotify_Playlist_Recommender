from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json

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

@app.get("/")
async def root():
    return {"message": "Hello from ToDo API!"}

@app.get("/square/{num}")
async def get_task(num):
    # Get the task from the table.
    return {int(num)**2}

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