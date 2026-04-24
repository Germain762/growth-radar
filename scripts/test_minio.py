"""Test MinIO connectivity and bucket access from Python."""
import boto3
from botocore.client import Config

# Config S3 pointée vers MinIO local
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",  # MinIO ignore mais boto3 exige un region
)

# Lister les buckets
response = s3.list_buckets()
buckets = [b["Name"] for b in response["Buckets"]]

print(f"✅ Connected to MinIO")
print(f"📦 Buckets found: {buckets}")

expected = {"bronze", "silver", "gold"}
missing = expected - set(buckets)
if missing:
    print(f"❌ Missing buckets: {missing}")
else:
    print(f"✅ All expected buckets are present")

# Test d'écriture/lecture
s3.put_object(Bucket="bronze", Key="_test/hello.txt", Body=b"Hello Growth Radar!")
obj = s3.get_object(Bucket="bronze", Key="_test/hello.txt")
content = obj["Body"].read().decode()
print(f"✅ Write/read test OK: {content!r}")

# Cleanup
s3.delete_object(Bucket="bronze", Key="_test/hello.txt")
print(f"✅ Cleanup OK")
