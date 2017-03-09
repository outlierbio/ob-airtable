import hashlib
import os
import os.path as op
from urllib.parse import urlparse
import warnings

S3_BUCKET = os.environ.get('AIRTABLE_BUCKET')
S3_FOLDER = os.environ.get('AIRTABLE_FOLDER') or 'attachments/'

try:
    import boto3
except ImportError:
    warnings.warn('boto3 not installed. No support for S3 attachments')

s3 = boto3.client('s3')


def path_to_bucket_and_key(path):
    (scheme, netloc, path, params, query, fragment) = urlparse(path)
    path_without_initial_slash = path[1:]
    return netloc, path_without_initial_slash


def calc_md5(fpath):
    hash_md5 = hashlib.md5()
    with open(fpath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def upload_to_s3_as_md5_hash(fpath, bucket=S3_BUCKET, prefix=S3_FOLDER):
    """Upload file to S3 with MD5 hash as key name

    Returns S3 path to key
    """
    md5 = calc_md5(fpath)
    ext = op.splitext(fpath)[1]
    key = prefix + md5 + ext
    s3.upload_file(fpath, bucket, key, ExtraArgs={'ACL': 'public-read'})
    url = 'https://{bucket}.s3.amazonaws.com/{key}'.format(
        bucket=bucket, key=key)
    return url