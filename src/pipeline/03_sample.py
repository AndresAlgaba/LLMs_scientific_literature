import os
import pandas as pd

base_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, "../..", "data")

df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_Full.tsv"),
    sep="\t",
)

# Needs to have DOI
df = df[df['DOI'].notna()]

# needs to be a journal paper
df = df[df['DocType'] == 'Journal']

# Needs to have >= 3 refs
df = df[df['Reference_Count'] >= 3]

# Needs to have <= 54 refs
df = df[df['Reference_Count'] <= 54]

# Needs to have >= 1 citation(s)
df = df[df['Citation_Count'] >= 1]

# between 1999 and 2023
df = df[(df['Year'] >= 1999) & (df['Year'] <= 2023)]

# needs to have 'top' field
df = df[df['Field_Type'].str.contains('Top', na=False)]

# Needs to be in Q1 journal
journal_df=pd.DataFrame()
for year in range(1999, 2024):
    journal_chunk=pd.read_csv(os.path.join(data_dir, "Journal_Ranking/scimagojr "+str(year)+".csv"), sep=';')
    journal_chunk=journal_chunk[['Issn', 'SJR Best Quartile']]
    journal_chunk['year']=year
    journal_df=pd.concat([journal_df, journal_chunk], ignore_index=True)

journal_df.reset_index(drop=True, inplace=True)

mask = journal_df['SJR Best Quartile'].isin(['Q1'])
journal_df = journal_df[mask]
journal_df.reset_index(drop=True, inplace=True)

journal_df[['ISSN1','ISSN2']] = journal_df['Issn'].str.split(', ', expand=True)

sciscinet_journals_df = pd.read_csv(
    os.path.join("SciSciNet_Journals.tsv"),
    sep="\t",
)
sciscinet_journals_df["ISSN"] = sciscinet_journals_df["ISSN"].str.replace('-', '')
issn1 = (sciscinet_journals_df["JournalID"][sciscinet_journals_df["ISSN"].isin(journal_df['ISSN1'])]).to_list()
issn2 = (sciscinet_journals_df["JournalID"][sciscinet_journals_df["ISSN"].isin(journal_df['ISSN2'])]).to_list()
journal_ids = issn1 + issn2

df = df[df['JournalID'].isin(journal_ids)]

# take random sample of remaining papers
print(len(df))
sample = df.sample(n=25000, random_state=42)
sample.to_csv(os.path.join(data_dir, "SciSciNet_Sample.csv"), index=False)
