
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import numpy as np
import json

import sqlalchemy as db
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd

import os




from nlpForSpotify import nl2features
from QueryBuilder import build_query



engine = db.create_engine('mysql+pymysql://root:password@localhost:3306/351_proj_db2')
conn = engine.connect()

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

@app.get("/tracks/{lim}")
async def get_tracks(lim):
    """
    Get 'lim' number of tracks from db (test)
    """
    query = f"SELECT track_name, track_id FROM track limit {str(lim)}"

    conn = engine.connect()

    temp = conn.execute(query).fetchall()
    conn.close()
    return {'tracks' : temp}




@app.get("/find-playlists-from-nl/{q}")
async def foo(q):
    q = build_query( nl2features(q.replace('`', ''))).replace('`', '')
    temp = conn.execute(q).fetchall()
 #   print(temp)

  #  for i in temp:
   #     id1 = i['mpd_id']

    return temp