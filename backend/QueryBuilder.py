from pprint import pprint

MAGIC = 0.2
LIMIT = 10

# LIAM JESKE

example =  {'acousticness': 0,
            'artists': ['Katy Perry', 'Lady Gaga', 'Drake'],
            'danceability': 1.0,
            'energy': 1.0,
            'genres': ['hip hop'],
            'instrumentalness': 0,
            'liveliness': 0,
            'loudness': 0,
            'speechiness': 0,
            'tempo': 1.0,
            'valence': 0}


def audio_feature_query(k, v):
    playlist_k = k + "_avg"
    if v == 0:
        return ""
    if v == -1:
        return f"{playlist_k} < {MAGIC}"
    if v == 1:
        return f"{playlist_k} >= {MAGIC}"


def sepOR(conds):
    if len(conds) > 1:
        ret = ""
        for i in conds[:-1]:
            ret += i + "  OR "

        return ret + conds[-1]
    else:
        return conds


def build_query(example, MAGIC = 0.2, LIMIT = 10):
    conds = []
    for i in example.keys():
        if i not in ['genres', 'artists']:
            b = example.get(i)
            if b != 0:
                conds.append(audio_feature_query(i, b))
        
    if len(example['artists']) > 1:
        conds.append(f"aname in {str(tuple(example['artists']))} ")
    if len(example['artists']) == 1:
        conds.append(f"aname in ('{example['artists'][0]}')")



    # GENRES NOT INCORPORATED YET

    base = fr"""
            SELECT mpd_id, pname
            FROM master2
            WHERE {sepOR(conds)}
            GROUP BY mpd_id, pname
            ORDER BY count(aname) desc
            LIMIT {LIMIT}
            """
    
    return base


print(build_query(example))