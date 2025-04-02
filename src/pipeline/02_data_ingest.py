import os
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import concurrent.futures


INDEX_NAME = "sciscinet"
CHUNK = 10000
BATCH_SIZE = 1000

base_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, "../..", "data")

client = Elasticsearch(
    hosts=os.getenv("ELASTIC_CLOUD_ENDPOINT"),
    api_key=os.getenv("ELASTIC_API_KEY"),
    request_timeout=1000,  # Default is 10
)

columns_to_load = [
    'PaperID',
    'Author_Name',
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
    'Citation_Count',
    'C10',
    'Reference_Count',
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
    'PaperTitle'
]

def process_chunk(df):
    for _, row in df.iterrows():
        doc = {
            "_op_type": "index",
            "_index": f"{INDEX_NAME}",
            "_id": row['PaperID'],
            "_source": {
                "PaperID": int(row['PaperID']),
                "Authors": row['Author_Name'] if pd.notna(row['Author_Name']) else None,
                "FieldID": row['FieldID'] if pd.notna(row['FieldID']) else None,
                "FieldType": row['Field_Type'] if pd.notna(row['Field_Type']) else None,
                "Hit1pct": row['Hit_1pct'] if pd.notna(row['Hit_1pct']) else None,
                "Hit5pct": row['Hit_5pct'] if pd.notna(row['Hit_5pct']) else None,
                "Hit10pct": row['Hit_10pct'] if pd.notna(row['Hit_10pct']) else None,
                "Cf": row['C_f'] if pd.notna(row['C_f']) else None,
                "DOI": row['DOI'] if pd.notna(row['DOI']) else None,
                "DocType": row['DocType'] if pd.notna(row['DocType']) else None,
                "Title": row['PaperTitle'] if pd.notna(row['PaperTitle']) else None,
                "Year": int(row['Year']) if pd.notna(row['Year']) else None,
                "JournalID": int(row['JournalID']) if pd.notna(row['JournalID']) else None,
                "ConferenceSeriesID": int(row['ConferenceSeriesID']) if pd.notna(row['ConferenceSeriesID']) else None,
                "Citations": int(row['Citation_Count']) if pd.notna(row['Citation_Count']) else None,
                "References": int(row['Reference_Count']) if pd.notna(row['Reference_Count']) else None,
                "C5": int(row['C5']) if pd.notna(row['C5']) else None,
                "C10": int(row['C10']) if pd.notna(row['C10']) else None,
                "TeamSize": int(row['Team_Size']) if pd.notna(row['Team_Size']) else None,
                "InstitutionCount": int(row['Institution_Count']) if pd.notna(row['Institution_Count']) else None,
                "Disruption": int(row['Disruption']) if pd.notna(row['Disruption']) else None,
                "Atyp_10pct_Z": int(row['Atyp_10pct_Z']) if pd.notna(row['Atyp_10pct_Z']) else None,
                "AtypPairs": int(row['Atyp_Pairs']) if pd.notna(row['Atyp_Pairs']) else None,
                "AtypMedianZ": int(row['Atyp_Median_Z']) if pd.notna(row['Atyp_Median_Z']) else None,
                "SB_B": int(row['SB_B']) if pd.notna(row['SB_B']) else None,
                "SB_T": int(row['SB_T']) if pd.notna(row['SB_T']) else None,
                "PatentCount": int(row['Patent_Count']) if pd.notna(row['Patent_Count']) else None,
                "NewsfeedCount": int(row['Newsfeed_Count']) if pd.notna(row['Newsfeed_Count']) else None,
                "TweetCount": int(row['Tweet_Count']) if pd.notna(row['Tweet_Count']) else None,
                "NCTCount": int(row['NCT_Count']) if pd.notna(row['NCT_Count']) else None,
                "NIHCount": int(row['NIH_Count']) if pd.notna(row['NIH_Count']) else None,
                "NSFCount": int(row['NSF_Count']) if pd.notna(row['NSF_Count']) else None,
                "WSB_mu": float(row['WSB_mu']) if pd.notna(row['WSB_mu']) else None,
                "WSB_sigma": float(row['WSB_sigma']) if pd.notna(row['WSB_sigma']) else None,
                "WSB_Cinf": float(row['WSB_Cinf']) if pd.notna(row['WSB_Cinf']) else None,
                "_reduce_whitespace": True,
            }
        }
        doc['_source'] = {k: v for k, v in doc['_source'].items() if v is not None}
        yield doc

def bulk_insert(actions):
    try:
        helpers.bulk(client, actions, chunk_size=BATCH_SIZE, pipeline="ent-search-generic-ingestion")
    except Exception as e:
        print(f"Error in bulk insert: {e}")

def main():
    if not client.indices.exists(index=INDEX_NAME):
        print("Creating index")
        client.indices.create(index=INDEX_NAME)

    chunks = pd.read_csv(
        os.path.join(data_dir, "SciSciNet_Full.tsv"),
        sep="\t",
        usecols=columns_to_load,
        chunksize=CHUNK,
    )

    with tqdm(total=134090311) as pbar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for df in chunks:
                pbar.update(len(df))
                if client.exists(index="sciscinet", id=df["PaperID"].iloc[0]):
                    continue
                actions = process_chunk(df)
                future = executor.submit(bulk_insert, actions)
                futures.append(future)

            concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
