import os
import pandas as pd


base_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, "../..", "data")

df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_PaperReferences.tsv"),
    sep="\t",
)

ids = [int(id.replace(".txt", "")) for id in os.listdir(os.path.join(data_dir, f"/abstracts"))]
subset_df = df[df["Citing_PaperID"].isin(ids)]

full_df = pd.read_csv(
    os.path.join(data_dir, "SciSciNet_Full.tsv"),
    sep="\t",
)

final_df = subset_df.merge(
    full_df,
    left_on="Cited_PaperID",
    right_on="PaperID",
    how="left",
)

final_df.to_csv(
    os.path.join(data_dir, "ground_truth_references.csv"),
    index=False
)
