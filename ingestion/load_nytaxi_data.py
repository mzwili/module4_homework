import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
from dotenv import load_dotenv

# Load .env file
load_dotenv()


BUCKET_NAME = os.getenv("GCS_BUCKET")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

if not BUCKET_NAME:
    print("ERROR: GCS_BUCKET environment variable not set.")
    sys.exit(1)

if not PROJECT_ID:
    print("ERROR: GCP_PROJECT_ID environment variable not set.")
    sys.exit(1)

client = storage.Client(project=PROJECT_ID)

# Taxi types and years required for homework
SERVICE_TYPES = ["yellow", "green"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data")

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def generate_download_tasks():
    tasks = []
    for service in SERVICE_TYPES:
        for year in YEARS:
            for month in MONTHS:
                tasks.append((service, year, month))
    return tasks


def download_file(task):
    service, year, month = task
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}/download/{service}/{file_name}"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is not accessible.")
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Upload failed: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    tasks = generate_download_tasks()

    # Download in parallel
    with ThreadPoolExecutor(max_workers=6) as executor:
        file_paths = list(executor.map(download_file, tasks))

    # Upload in parallel
    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("All files processed and verified.")
