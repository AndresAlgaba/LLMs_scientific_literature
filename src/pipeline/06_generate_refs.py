import os
import json
import pandas as pd

from openai import OpenAI

from config import SYSTEM_PROMPT


base_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, "../..", "data")

client = OpenAI()

df = pd.read_csv(os.path.join(data_dir, "SciSciNet_Sample_Journals.csv"))

requests = []
for paper_file in os.listdir(os.path.join(data_dir, f"/abstracts")):
    paper_id = int(paper_file.replace(".txt", ""))
    info = df[df["PaperID"] == paper_id]

    with open(os.listdir(os.path.join(data_dir, f"/abstracts/{paper_file}"))) as f:
        text = f.read()

    message = f"Title: {info['PaperTitle'].values[0]} \n\n Authors: {info['Author_Name'].values[0]} \n\n Year: {int(info['Year'].values[0])} \n\n Venue: {info['Journal_Name'].values[0]} \n\n Abstract: {text}"

    request = {
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-2024-08-06",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT.format(n=int(info["Reference_Count"].values[0]))},
                {"role": "user", "content": message},
            ],
            "max_tokens": 16384,
        },
        "custom_id": str(paper_id) + "_run_1",
    }

    requests.append(request)

with open('large_batch_requests.jsonl', 'w') as outfile:
    for request in requests:
        json.dump(request, outfile)
        outfile.write('\n')

batch_input_file = client.files.create(
  file=open("large_batch_requests.jsonl", "rb"),
  purpose="batch"
)

batch_info = client.batches.create(
    input_file_id=batch_input_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h",
    metadata={
      "description": "Run 1 on GPT-4o on abstracts",
    }
)

# check status - platform
# client.batches.retrieve(batch_info.id).request_counts

# download from platform
# file_response = client.files.content(client.batches.retrieve(batch_info.id).output_file_id)
# print(file_response.text)
