import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

BUCKET = os.getenv('S3_BUCKET_NAME')
LOCAL_PATH = Path('data/raw/olist')
S3_PREFIX = 'bronze/olist/raw'

files = list(LOCAL_PATH.glob('*.csv'))
print(f"{len(files)} dosya yüklenecek...\n")

for file in files:
    s3_key = f"{S3_PREFIX}/{file.name}"
    print(f"Yükleniyor: {file.name} → s3://{BUCKET}/{s3_key}")
    s3.upload_file(str(file), BUCKET, s3_key)
    print(f"  ✓ Tamamlandı")

print(f"\nTüm dosyalar s3://{BUCKET}/{S3_PREFIX}/ altına yüklendi.")
