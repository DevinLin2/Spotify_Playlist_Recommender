### GET EVERYTHING READY FOR INPUT

# author: Alexis Corona

import spacy
import pandas
import nltk
nltk.download('wordnet')
nltk.download('omw-1.4')
from nltk.corpus import wordnet

artists_names = pandas.read_csv("artists_unique.csv")["artist_name"].values
genre_names = pandas.read_csv("genres1_unique.csv")["top_genre_1"].values
nlp = spacy.load("en_core_web_sm")

categories = ["acousticness",
               "danceability",
               "energy",
               "instrumentalness",
               "liveliness",
               "loudness",
               "speechiness",
               "tempo", 
               "valence"]

keywords_pos = {"acousticness" : set(["musical", "choral", "acoustic", "cappella", "air", "chamber", "classical", "classic", "folk", "harmonic", "resonant", "rustic", "clean", "classic"]),
               "danceability" : set(["groovy", "funky", "funk", "catchy", "listenable", "dance", "danceable", "hummable", "tuneful", "jangly", "rhythmic"]),
               "energy" : set(["inciting", "glorious", "exciting", "exhilarating", "energetic", "motivational", "frantic", "work", "workout", "powerful", "snappy", "zestful", "tireless", "spirited", "kinetic", "speedy", "sprightly", "active", "brisk", "peppy", "fast", "exercise", "pump", "pumped", "dance", "danceable", "party", "lively", "peppy", "agressive", "dynamic"]),
               "instrumentalness" : set(["instrumental", "folk", "rustic", "clean", "classic", "chamber"]),
               "liveliness" : set(["frantic", "lively", "peppy", "active", "upbeat", "brisk", "snappy", "active", "happy"]),
               "loudness" : set(["glorious", "sonic", "fortissimo", "dramatic", "hard", "frantic", "loud", "booming", "noisy", "clamorous", "intense"]),
               "speechiness" : set(["poetic", "lyrical", "motivational", "breathy", "speechy", "verbal", "capella", "singing", "sing"]),
               "tempo" : set(["glorious", "frantic", "fast", "uptempo", "party", "soaring", "hyper"]),
               "valence" : set(["romantic", "uplifting", "inspiring", "healing", "soothing", "warm", "motivational", "happy", "breathtaking", "glad", "good", "beautiful", "beach", "paradise", "relaxing"])}

keywords_neg = {"acousticness" : set(["electric", "electronic", "beeping", "chiptune", "computer", "computerized"]),
               "danceability" : set(["sophisticated", "enigmatic", "irregular", "strange", "contemporary", "relaxing"]),
               "energy" : set(["rainy", "rain", "slow", "sleepy", "tired", "rest", "restful", "lullaby", "beach", "relaxing", "forest"]),
               "instrumentalness" : set(["simple", "choral", "choir", "speechy", "verbal", "capella", "singing", "sing"]),
               "liveliness" : set(["stufy", "rain", "sleepy", "tired", "rest", "restful", "lullaby", "chill", "calm", "beach", "paradise"]),
               "loudness" : set(["light", "gentle", "faint", "soft", "smooth", "study", "quiet", "sneaky", "calm"]),
               "speechiness" : set(["instrumental", "folk", "rustic", "clean", "classic", "chamber"]),
               "tempo" : set(["romantic", "mellow", "soothing", "warm", "smooth", "slow", "slowed", "chill", "calm," "downtempo", "calm"]),
               "valence" : set(["emotional", "moody", "somber", "melancholy", "sad", "mournful", "bad", "blue", "depressing"])}

keywords_increase = set(["very", "extremely", "really"])
keywords_reverse = set(["not", "no"])

for i in range(0, 1):
    for category in categories:
        pos_base = keywords_pos[category]
        neg_base = keywords_neg[category]
        pos = set()
        neg = set()
        for word in pos_base:
            for syn in wordnet.synsets(word):
                for lm in syn.lemmas():
                    pos.add(lm.name())
                    if lm.antonyms():
                        neg.add(lm.antonyms()[0].name())
        for word in neg_base:
            for syn in wordnet.synsets(word):
                for lm in syn.lemmas():
                    neg.add(lm.name())
                    if lm.antonyms():
                        pos.add(lm.antonyms()[0].name())
        keywords_pos[category].update(pos)
        keywords_neg[category].update(neg)
        
    increase_base = keywords_increase.copy()
    reverse_base = keywords_reverse.copy()
    for word in increase_base:
        for syn in wordnet.synsets(word):
            for lm in syn.lemmas():
                keywords_increase.add(lm.name())

    for word in reverse_base:
        for syn in wordnet.synsets(word):
            for lm in syn.lemmas():
                keywords_reverse.add(lm.name())

### USER GIVES INPUT

def nl2features(iput):
#iput = "relaxing beach playlist"

    names_found = []
    for artist in artists_names:
        if artist.lower() in iput.lower():
            index = iput.lower().index(artist.lower())
            if (index == 0 or not iput[index - 1].isalpha()) \
            and (index + len(artist.lower()) == len(iput) or not iput[index + len(artist.lower())].isalpha()):
                names_found.append(artist)
                iput = iput[:index] + iput[index + len(artist.lower()):]

    genres_found = []
    for genre in genre_names:
        if genre.lower() in iput.lower():
            index = iput.lower().index(genre.lower())
            if (index == 0 or not iput[index - 1].isalpha()) \
            and (index + len(genre.lower()) == len(iput) or not iput[index + len(genre.lower())].isalpha()):
                genres_found.append(genre)

    features_dict = {"acousticness":0,
                "danceability":0,
                "energy":0,
                "instrumentalness":0,
                "liveliness":0,
                "loudness":0,
                "speechiness":0,
                "tempo":0,
                "valence":0}


    doc = nlp(iput)
    mult = 1
    found = False

    for token in doc:
        for category in categories:
            if token.lemma_ in keywords_pos[category]:
                features_dict[category] += mult
                mult = 1
            if token.lemma_ in keywords_neg[category]:
                features_dict[category] -= mult
                mult = 1
            if token.lemma_ in keywords_reverse or token.lemma_ in keywords_increase:
                if token.lemma_ in keywords_reverse:
                    mult = -1 * mult
                if token.lemma_ in keywords_increase:
                    mult = mult + mult/abs(mult)
            else:
                mult = 1

    for feature in features_dict:
        if (features_dict[feature] != 0):
            features_dict[feature] = features_dict[feature] / abs(features_dict[feature])

    features_dict['artists'] = names_found
    features_dict['genres'] = genres_found

    
    return features_dict

nl2features("relaxing beach party playlist with Katy Perry")
print("Done")