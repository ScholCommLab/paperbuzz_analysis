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

# +
cr_works = "http://api.crossref.org/works/"
email = "asura_enkhbayar@sfu.ca"

ONE_SEC = 1
CALLS = 10

@sleep_and_retry
@limits(calls=CALLS, period=ONE_SEC)
def call_cr_api(doi, session):
    future = session.head(cr_works + doi, headers={'mailto':email}, timeout=5)
    return future

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


# +
df = pd.read_csv("dois_20190618_cleaned.csv", parse_dates=['dcdate'])
df.set_index("id", inplace=True)
df.dcdate.replace("nan", np.nan, inplace=True)

df.sort_values(by="dcdate", ascending=False, inplace=True)
# -
out_exists = False
if os.path.isfile('./out.csv'):
    out_exists = True
 
    out = pd.read_csv("out.csv", index_col="id", dtype={'cr_works':str})
    out['timestamp'] = pd.to_datetime(out['timestamp'])
    out['date'] = pd.to_datetime(out['date'], errors = 'coerce')

# ddf = df.sample(1000)
if out_exists:
    ddf = df[~df.doi.isin(out.doi)]
else:
    ddf = df

df[~df.doi.isin(out.doi)]

ddf

ddf.index

# +
batch_size = 50
batches = grouper(ddf.index, batch_size)

with open('out.csv', 'a+') as csvfile:
    csvwriter = csv.writer(csvfile)
    if not out_exists:
        csvwriter.writerow(["id", "doi", "date", "cr_works", "timestamp"])

    for batch in tqdm(batches, total=np.ceil(len(ddf)/batch_size), desc="Overall"):
        futures = []
        subdf = ddf.reindex(batch)

        with FuturesSession(max_workers=8) as session:
            for ix, row in tqdm(subdf.iterrows(), total=len(subdf), desc="Requests", leave=False):
                doi = row['doi']
                ts = datetime.now().isoformat()

                future = call_cr_api(str(doi), session)
                futures.append((ix, ts, future))

        for ix, ts, future in tqdm(futures, desc="Responses", leave=False):
            sc = None
            try:
                resp = future.result(timeout=5)
                sc = int(resp.status_code)
            except Exception as e:
                    sc = e.__class__.__name__

            row = ddf.loc[ix]
            row['cr_works'] = sc
            row['timestamp'] = ts
            csvwriter.writerow([ix] + row[['doi', 'dcdate', 'cr_works', 'timestamp']].tolist())        
