import os
from pathlib import Path

env_file = Path(".env")
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
WIDTH, HEIGHT = 64, 48
NUM_PIXELS = WIDTH * HEIGHT


def get_s3_client():
    import boto3
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def get_s3_bucket():
    return os.environ["S3_BUCKET_NAME"]
