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

try:
    __IPYTHON__
except NameError:
    from tqdm import tqdm
else:
    from tqdm import tqdm_notebook as tqdm
# -

csv.field_size_limit(sys.maxsize)

input_csv = "paperbuzz.csv"
output_csv = "paperbuzz_metrics.csv"

print("Counting lines using 'wc -l'")
line_count = int(subprocess.check_output(['wc', '-l', "paperbuzz.csv"]).split()[0]) - 1
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
out_cols = ["id", "doi", "date"] + sources

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
                metrics = {k:0 for k in sources}
                if 'altmetrics_sources' in j:
                    for s in j['altmetrics_sources']:
                        metrics[s['source_id']] = int(s['events_count'])
                outrow = [row['id'], row['doi'], row['date']]
                for s in sources:
                    outrow.append(metrics[s])
                csv_writer.writerow(outrow)
