
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
    print(type(temp))
    print(type(temp[0]))

    lst = []
    for i in temp:
        d = dict()
        d['mpd_id'] = i['mpd_id']
        d['playlist_name'] = i['pname']

        temp2 = conn.execute(
            f"""
            SELECT distinct track_id, track_name, aname
            FROM master2
            WHERE mpd_id = {d['mpd_id']}
            LIMIT 12;
            """)
        
        lst2 = []

        for j in temp2:
            d2 = dict()
            #d2['mpd_id'] = d['mpd_id']
            #d2['playlist_name'] = i['pname']
            d2['track_name'] = j['track_name']
            d2['aname'] = j['aname']
            d2['track_id'] = j['track_id']

            lst2.append(d2)

        d['songs'] = lst2

        lst.append(d)


    return lst