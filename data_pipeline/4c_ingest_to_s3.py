import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import databento as db
import requests

from config import get_s3_client, get_s3_bucket

parser = argparse.ArgumentParser()
parser.add_argument("--job-id", required=True)
parser.add_argument("--prefix", required=True)
parser.add_argument("--workers", type=int, default=4)
args = parser.parse_args()

client = db.Historical(os.environ["DATABENTO_API_KEY"])
s3 = get_s3_client()
bucket = get_s3_bucket()

# Databento why don't you have a .get_job() method?
job = next((j for j in client.batch.list_jobs() if j["id"] == args.job_id), None)

if job["state"] != "done":
    print(f"Job {job['id']} is not done (status: {job['state']})")
    exit(1)
print(f"Found job {job['id']}")

files_by_name = {f["filename"]: f for f in client.batch.list_files(args.job_id)}
data_files = [name for name in files_by_name if name.endswith(".dbn.zst")]
print(f"Files to ingest: {len(data_files)}")

symbology_file = files_by_name.get("symbology.json")
if symbology_file:
    resp = requests.get(symbology_file["urls"]["https"], auth=(os.environ["DATABENTO_API_KEY"], ""))
    raw = resp.json()
    s3.put_object(Bucket=bucket, Key=f"{args.prefix}/symbology_{args.job_id}.json", Body=json.dumps(raw))
    print(f"Uploaded symbology_{args.job_id}.json")
else:
    print("No symbology file found (something has probably gone wrong)")


def ingest_file(filename):
    s3_key = f"{args.prefix}/{filename}"
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        return f"SKIP: {filename}"
    except:
        pass
    client.batch.download(job_id=args.job_id, filename_to_download=filename, output_dir="/tmp")
    local_path = f"/tmp/{args.job_id}/{filename}"
    s3.upload_file(local_path, bucket, s3_key)
    os.remove(local_path)
    return f"UPLOADED: {filename}"


with ThreadPoolExecutor(max_workers=args.workers) as executor:
    futures = {executor.submit(ingest_file, f): f for f in data_files}
    for future in as_completed(futures):
        print(future.result())

print("Done!")
