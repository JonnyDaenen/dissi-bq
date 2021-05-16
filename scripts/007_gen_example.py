import json
import random
import sys
import pathlib

bucket = None

if len(sys.argv) > 1:
    bucket = sys.argv[1]
    print(f'using bucket {bucket}')


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    from google.cloud import storage
    """
    Uploads a file to the bucket.
    Src: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))


def decision(probability):
    return random.random() < probability


def gen_data(prob=0.1):
    data = {
        'always_there': "hello!"
    }
    if prob > 0.01:
        if decision(prob):
            data['integer'] = random.choice([1, 2, 3, 4, 5])
        if decision(prob):
            data['string'] = random.choice(['hello', 'world'])
        if decision(prob):
            data['boolean'] = random.choice([True, False])
        if decision(prob):
            data['complex'] = gen_data(prob / 2)
        if decision(prob):
            data['list'] = random.choice([[1, 2, 3, 4, 5], [1, 2, 3]])

    return data


pathlib.Path("./examples/large").mkdir(parents=True, exist_ok=True)

# Generate multiple files
for i in range(10):
    # Generate data points
    json_data = [gen_data(0.4) for _ in range(100000)]

    # Write locally
    filename = f'examples/large/large_{i}.jsonl'
    with open(filename, 'w') as outfile:
        for entry in json_data:
            json.dump(entry, outfile)
            outfile.write('\n')

    # Write to bucket
    if bucket:
        upload_blob(bucket, filename, filename)
