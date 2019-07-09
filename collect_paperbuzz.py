# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 1.0.5
#   kernelspec:
#     display_name: altmetrics
#     language: python
#     name: altmetrics
# ---

# +
import csv
from datetime import datetime
from itertools import zip_longest

import numpy as np
import pandas as pd
import requests
from ratelimit import limits, sleep_and_retry
from requests_futures.sessions import FuturesSession

try:
    __IPYTHON__
except NameError:
    from tqdm import tqdm
else:
    from tqdm import tqdm_notebook as tqdm
    
import os  

import json

# +
cr_works = "https://api.paperbuzz.org/v0/doi/"
email = "asura_enkhbayar@sfu.ca"

ONE_SEC = 1
CALLS = 10


# +
@sleep_and_retry
@limits(calls=CALLS, period=ONE_SEC)
def call_paperbuzz(doi, session):
    future = session.get(cr_works + doi, params={'email':email}, timeout=15)
    return future

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


# -

results_exist = False
if os.path.isfile('./paperbuzz.csv'):
    results_exist = True
 
    out = pd.read_csv("paperbuzz.csv", dtype={'status':str})
    out.set_index("id", inplace=True)
    out['timestamp'] = pd.to_datetime(out['timestamp'])
    out['date'] = pd.to_datetime(out['date'], errors = 'coerce')

# +
df = pd.read_csv("out.csv", dtype={'cr_works':str})
df.set_index("id", inplace=True)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['date'] = pd.to_datetime(df['date'], errors = 'coerce')

df = df[~df.date.isnull()]
# -
existing_dois = df[df.cr_works == "200"]
# existing_dois = existing_dois.sample(100)

with FuturesSession() as session:
    f = call_paperbuzz("10.1038/nature12373", session)
    x = f.result()

# +
columns = ["doi", "date", "status", "response", "timestamp"]

batch_size = 50
batches = grouper(existing_dois.index, batch_size)

with open('paperbuzz.csv', 'a+') as csvfile:
    csvwriter = csv.writer(csvfile)
    if not results_exist:
        csvwriter.writerow(["id"] + columns)

    for batch in tqdm(batches, total=np.ceil(len(existing_dois)/batch_size), desc="Overall"):
        futures = []
        subdf = existing_dois.reindex(batch)

        with FuturesSession(max_workers=8) as session:
            for ix, row in tqdm(subdf.iterrows(), total=len(subdf), desc="Requests", leave=False):
                doi = row['doi']
                ts = datetime.now().isoformat()

                future = call_paperbuzz(str(doi), session)
                futures.append((ix, ts, future))

        for ix, ts, future in tqdm(futures, desc="Responses", leave=False):
            sc = None
            response_json = None
            try:
                resp = future.result(timeout=5)
                sc = int(resp.status_code)
                try:
                    response_json = resp.json()
                except Exception as e:
                    response_json = {}
            except Exception as e:
                    sc = e.__class__.__name__

            row = existing_dois.loc[ix].copy()
            row['status'] = sc
            if response_json:
                row['response'] = json.dumps(response_json)
            else:
                row['response'] = str(response_json)
            row['timestamp'] = ts
            csvwriter.writerow([ix] + row[columns].tolist())        
