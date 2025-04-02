import os
import numpy as np
import pandas as pd
from thefuzz import fuzz, process
from elasticsearch import Elasticsearch

SIZE = 3

client = Elasticsearch(
    hosts=os.getenv("ELASTIC_CLOUD_ENDPOINT"),
    api_key=os.getenv("ELASTIC_API_KEY"),
)

df = pd.read_csv("generated_refs.csv")

df[
    [
        'PaperID',
        'SciSciNet_Title',
        'SciSciNet_Authors',
        'FieldID',
        'Field_Type',
        'Hit_1pct',
        'Hit_5pct',
        'Hit_10pct',
        'C_f',
        'DOI',
        'DocType',
        'Year',
        'JournalID',
        'ConferenceSeriesID',
        'Citations',
        'C10',
        'References',
        'C5',
        'Team_Size',
        'Institution_Count',
        'Disruption',
        'Atyp_10pct_Z',
        'Atyp_Pairs',
        'Atyp_Median_Z',
        'SB_B',
        'SB_T',
        'Patent_Count',
        'Newsfeed_Count',
        'Tweet_Count',
        'NCT_Count',
        'NIH_Count',
        'NSF_Count',
        'WSB_mu',
        'WSB_sigma',
        'WSB_Cinf',
    ]
] = np.nan

df = df.astype(
    {
        'C_f': 'object',
        'SciSciNet_Authors': 'object',
        'Hit_1pct': 'object',
        'Hit_5pct': 'object',
        'Hit_10pct': 'object',
        'FieldID': 'object',
        'DocType': 'object',
        'DOI': 'object',
        'SciSciNet_Title': 'object',
        'Field_Type': 'object',
    }
)

for i, row in df.iterrows():
    query_body = {
        "size": SIZE,
        "query": {
            "match_phrase": { 
                "Title": f"{row['Title']}" 
            }
        }
    }

    response = client.search(index="sciscinet", body=query_body)

    if response['hits']['total']['value'] == 0:
        df.at[i, "Exists"] = 0.0
        continue

    titles = [hit['_source']['Title'].strip().lower() for hit in response['hits']['hits']]
    title_similarities = process.extract(row["Title"].strip().lower(), titles, scorer=fuzz.partial_ratio)

    authors = [hit['_source']['Authors'].strip().lower() for hit in response['hits']['hits']]
    author_similarities = process.extract(row["Authors"].strip().lower(), authors, scorer=fuzz.token_set_ratio)

    similarities = [(title[1]*0.5) + (author[1]*0.5) for title, author in zip(title_similarities, author_similarities)]

    best_match_index = max(range(len(similarities)), key=similarities.__getitem__)
    exists = (title_similarities[best_match_index][1] > 90) * (author_similarities[best_match_index][1] > 50)

    df.at[i, "Exists"] = exists
    df.at[i, "Title_score"] = title_similarities[best_match_index][1]
    df.at[i, "Authors_score"] = author_similarities[best_match_index][1]
    info = response['hits']['hits'][best_match_index]['_source']
    info['SciSciNet_Title'] = info.pop('Title')
    info['SciSciNet_Authors'] = info.pop('Authors')

    df.loc[i, info.keys()] = info.values()

df.to_csv("processed_generated_results.csv", index=False)
