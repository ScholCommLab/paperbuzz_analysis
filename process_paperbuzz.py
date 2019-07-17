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
import json
import subprocess
import sys
import datetime

try:
    __IPYTHON__
except NameError:
    from tqdm import tqdm
else:
    from tqdm import tqdm_notebook as tqdm
# -

csv.field_size_limit(sys.maxsize)

input_csv = "data/paperbuzz.csv"
output_csv = "data/paperbuzz_metrics.csv"

print("Counting lines using 'wc -l'")
line_count = int(subprocess.check_output(['wc', '-l', input_csv]).split()[0]) - 1
print("{} lines in CSV".format(line_count))

# +
sources = set()

with open(input_csv, "r") as input_f:
    csv_reader = csv.DictReader(input_f, delimiter=",")
    next(csv_reader)
    
    for row in tqdm(csv_reader, total=line_count, desc="Extracting sources"):
        r = row['response']
        if not r in ["None", "{}", ""]:
            j = json.loads(r)
            if 'altmetrics_sources' in j:
                for s in j['altmetrics_sources']:
                    sources.add(s['source_id'])

sources = list(sources)

# +
cols = ["id","doi","date","status","response","timestamp"]
meta_cols = ["article_type", "is_oa", "n_authors", "journal_title", "title"]
out_cols = ["id", "doi", "date"] + sources + meta_cols

with open(input_csv, "r") as input_f:
    csv_reader = csv.DictReader(input_f)
    next(csv_reader)

    with open(output_csv, "w") as output_f:
        csv_writer = csv.writer(output_f)
        csv_writer.writerow(out_cols)
        
        for row in tqdm(csv_reader, total=line_count, desc="Extracting metrics"):
            r = row['response']
            if not r in ["None", "{}", ""]:
                j = json.loads(r)
                outrow = [row['id'], row['doi'], row['date']]
                
                # Extract metrics
                metrics = {k:0 for k in sources}
                if 'altmetrics_sources' in j:
                    for s in j['altmetrics_sources']:
                        metrics[s['source_id']] = int(s['events_count'])
                for s in sources:
                    outrow.append(metrics[s])
                
                # Extract metadata
                metadata = {k:None for k in meta_cols}
                try:
                    metadata['article_type'] = j['metadata']['type']
                except:
                    pass
        
                try:
                    metadata['n_authors'] = len(j['metadata']['author'])
                except:
                    pass
                
                try:
                    metadata['journal_title'] = j['metadata']['container-title']
                except:
                    pass
                
                try:
                    metadata['title'] = j['metadata']['title']
                except:
                    pass
                
                try:
                    metadata['is_oa'] = j['open_access']['is_oa']
                except:
                    pass
                
                for m in meta_cols:
                    outrow.append(metadata[m])
                    
                csv_writer.writerow(outrow)
