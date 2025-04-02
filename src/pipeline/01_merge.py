import os
import gc
import pandas as pd


base_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, "../..", "data")

print("Start from paper ids and author ids")
paper_author_columns = [
    'PaperID',
    'AuthorID',
]
paper_author_dtypes = {
    'PaperID': 'uint32',
    'AuthorID': 'uint32',
}
paper_author_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_PaperAuthorAffiliations.tsv"),
    sep="\t",
    usecols=paper_author_columns,
    dtype=paper_author_dtypes,
)
print("remove multiple affiliations")
paper_author_df.drop_duplicates(subset=['PaperID', 'AuthorID'], keep='first', inplace=True)

print("add author names")
author_columns = [
    'AuthorID',
    'Author_Name',
    'H-index',
    'Productivity',
    'Average_C10',
]
author_dtypes = {
    'AuthorID': 'uint32',
    'Author_Name': 'string',
    'H-index': 'Int32',
    'Productivity': 'Int32',
    'Average_C10': 'float32',
}
author_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_Authors.tsv"),
    sep="\t",
    usecols=author_columns,
    dtype=author_dtypes,
)

paper_author_df.set_index('AuthorID', inplace=True, drop=False)
author_df.set_index('AuthorID', inplace=True)
sciscinet_df = paper_author_df.join(author_df, how='inner')  # remove authors without names

sciscinet_df = sciscinet_df.groupby('PaperID')['Author_Name'].agg(', '.join).reset_index()

print(len(sciscinet_df))
print(sciscinet_df.columns)

del paper_author_df
del author_df
gc.collect()

print("add field info to paper ids")
paper_field_columns = [
    'PaperID',
    'FieldID',
    'Hit_1pct',
    'Hit_5pct',
    'Hit_10pct',
    'C_f',
]
paper_field_dtypes = {
    'PaperID': 'uint32',
    'FieldID': 'uint32',
    'Hit_1pct': 'float32',
    'Hit_5pct': 'float32',
    'Hit_10pct': 'float32',
    'C_f': 'float32',
}
paper_field_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_PaperFields.tsv"),
    sep="\t",
    usecols=paper_field_columns,
    dtype=paper_field_dtypes,
)

field_columns = [
    'FieldID',
    'Field_Type',
]
field_dtypes = {
    'FieldID': 'uint32',
    'Field_Type': 'category',
}
field_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_Fields.tsv"),
    sep="\t",
    usecols=field_columns,
    dtype=field_dtypes,
)

paper_field_df.set_index('FieldID', inplace=True, drop=False)
field_df.set_index('FieldID', inplace=True)
joined_field_df = paper_field_df.join(field_df, how='left')  # keep all papers
joined_field_df = joined_field_df.groupby('PaperID')[['FieldID', 'Field_Type', 'Hit_1pct', 'Hit_5pct', 'Hit_10pct', 'C_f']].agg(lambda x: ', '.join(map(str, x))).reset_index()

del paper_field_df
del field_df
gc.collect()

sciscinet_df.set_index('PaperID', inplace=True, drop=False)
joined_field_df.set_index('PaperID', inplace=True)

sciscinet_df = sciscinet_df.join(joined_field_df, how='left')  # keep all papers

del joined_field_df
gc.collect()

print("add paper info")
paper_columns = [
    'PaperID',
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
]
paper_dtypes = {
    'PaperID': 'uint32',
    'DOI': 'string',
    'DocType': 'category',
    'Year': 'Int32',
    'JournalID': 'Int64',
    'ConferenceSeriesID': 'Int64',
    'Citation_Count': 'Int32',
    'C10': 'Int32',
    'Reference_Count': 'Int32',
    'C5': 'Int32',
    'Team_Size': 'Int32',
    'Institution_Count': 'Int32',
    'Disruption': 'float32',
    'Atyp_10pct_Z': 'float32',
    'Atyp_Pairs': 'Int32',
    'Atyp_Median_Z': 'float32',
    'SB_B': 'float32',
    'SB_T': 'float32',
    'Patent_Count': 'Int32',
    'Newsfeed_Count': 'Int32',
    'Tweet_Count': 'Int32',
    'NCT_Count': 'Int32',
    'NIH_Count': 'Int32',
    'NSF_Count': 'Int32',
    'WSB_mu': 'float32',
    'WSB_sigma': 'float32',
    'WSB_Cinf': 'float32',
}
paper_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_Papers.tsv"),
    sep="\t",
    usecols=paper_columns,
    dtype=paper_dtypes,
)

paper_df.set_index('PaperID', inplace=True)
sciscinet_df = sciscinet_df.join(paper_df, how='inner')

del paper_df
gc.collect()

print("add title info")
title_columns = [
    'PaperID',
    'PaperTitle',
]
title_dtypes = {
    'PaperID': 'uint32',
    'PaperTitle': 'string',
}
title_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_PaperDetails.tsv"),
    sep="\t",
    usecols=title_columns,
    dtype=title_dtypes,
)

title_df.set_index('PaperID', inplace=True)
sciscinet_df = sciscinet_df.join(title_df, how='inner')

del title_df
gc.collect()

sciscinet_df.to_csv(
    os.path.join(data_dir, "SciSciNet_Full.tsv"),
    sep="\t",
    index=False
)
