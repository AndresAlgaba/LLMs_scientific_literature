import os
import pandas as pd
import time

from crossref.restful import Works, Etiquette

from config import APPLICATION_NAME, EMAIL
from utils import semantic_scholar_search


base_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, "../..", "data")

df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_Sample.csv")
)

my_etiquette = Etiquette(
    application_name=APPLICATION_NAME,
    contact_email=EMAIL,
)
works = Works(etiquette=my_etiquette)

for _, row in df.iterrows():

    if len(os.listdir(os.path.join(data_dir, f"/abstracts"))) > 10000:
        break

    doi = row["DOI"]
    id = row["PaperID"]

    if os.path.exists(os.path.join(data_dir, f"/abstracts/{id}.txt")):
        print(f"{id} exists")
        continue

    data = works.doi(doi)
    time.sleep(1)

    if data is None:
        print(f"{id} is None in CrossRef")
        continue

    abstract = data.get("abstract")

    if abstract is None:
        print(f"{id} has no CrossRef abstract")
        title_list = data.get('title', [])
        first_title = title_list[0] if title_list else None

        if first_title is None:
            print(f"{id} has no title")
            continue

        results = semantic_scholar_search(first_title)
        time.sleep(1)

        if results is None:
            print(f"{id} is None in SemanticScholar")
            continue

        if results[0]["externalIds"].get("DOI") is None:
            print(f"{id} has no DOI in SemanticScholar")
            continue

        if doi.lower().strip() == results[0]["externalIds"].get("DOI").lower().strip():
            abstract = results[0].get("abstract")
            if abstract is None:
                print(f"{id} has no abstract in SemanticScholar")
                continue

            print(f"{id} has abstract in SemanticScholar")
            with open(f"data/abstracts/{id}.txt", "w") as f:
                f.write(abstract)
            continue

        else:
            print(f"{id} DOI does not match")
            print(f"DOI: {doi}")
            print(f"SemanticScholar DOI: {results[0]['externalIds'].get('DOI')}")
            continue

    print(f"{id} has abstract in CrossRef")
    with open(os.path.join(data_dir, f"/abstracts/{id}.txt"), "w") as f:
        f.write(abstract)
