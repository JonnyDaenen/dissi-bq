import json


with open('examples/base.json', 'r') as infile:
    json_data = json.load(infile)

with open('examples/base.jsonl', 'w') as outfile:
    for entry in json_data:
        json.dump(entry, outfile)
        outfile.write('\n')


with open('examples/modified.json', 'r') as infile:
    json_data = json.load(infile)

with open('examples/modified.jsonl', 'w') as outfile:
    for entry in json_data:
        json.dump(entry, outfile)
        outfile.write('\n')
