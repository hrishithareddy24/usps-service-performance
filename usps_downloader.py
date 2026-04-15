import requests
import os
import time
import json
import re
from google.cloud import storage

BUCKET_NAME = "usps-pipeline-data"
BASE_URL = "https://spm.usps.com/api/extract/download/{}"
TEMP_FILE = "/tmp/usps_temp.gz"
TRACKER_FILE = os.path.expanduser("~/Desktop/usps_tracker.txt")
INDEX_FILE = os.path.expanduser("~/Desktop/usps_files_index.json")

def load_tracker():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, "r") as f:
            return set(f.read().splitlines())
    return set()

def save_tracker(file_id):
    with open(TRACKER_FILE, "a") as f:
        f.write(f"{file_id}\n")

def upload_and_delete(local_file, filename):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    blob.upload_from_filename(local_file)
    os.remove(local_file)

def get_number(filename):
    nums = re.findall(r'\d+', filename)
    return int(nums[0]) if nums else 0

def run():
    downloaded = load_tracker()
    print(f"Starting... {len(downloaded)} files already done")

    with open(INDEX_FILE) as f:
        all_files = json.load(f)

    all_files_sorted = sorted(all_files, key=lambda x: get_number(x['path']))
    remaining = [f for f in all_files_sorted if f['path'] not in downloaded]
    print(f"Remaining: {len(remaining)} files to download")

    consecutive_errors = 0
    total_downloaded = 0

    for file_info in remaining:
        filename = file_info['path']
        url = BASE_URL.format(filename)
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                consecutive_errors += 1
                print(f"  Error on {filename}: {consecutive_errors} consecutive errors")
            else:
                with open(TEMP_FILE, "wb") as f:
                    f.write(r.content)
                upload_and_delete(TEMP_FILE, filename)
                save_tracker(filename)
                consecutive_errors = 0
                total_downloaded += 1
                print(f"  {filename} ({len(r.content)/1024/1024:.1f} MB) — total: {total_downloaded}")
                time.sleep(2)  # wait 2 seconds between downloads

        except Exception as e:
            consecutive_errors += 1
            print(f"  Error on {filename}: {consecutive_errors} consecutive errors")

        if consecutive_errors >= 10:
            print(f"\n  Too many errors — waiting 15 minutes...")
            time.sleep(900)
            consecutive_errors = 0
            print("  Resuming...")

    print("\nAll done!")

if __name__ == "__main__":
    run()
