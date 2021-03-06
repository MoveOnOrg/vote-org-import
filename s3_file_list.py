import boto3
from pywell.entry_points import run_from_cli


DESCRIPTION = 'List of files matching S3 bucket path.'

ARG_DEFINITIONS = {
    'AWS_ACCESS_KEY': 'AWS IAM key.',
    'AWS_SECRET_KEY': 'AWS IAM secret.',
    'S3_BUCKET': 'Name of bucket to search.'
}

REQUIRED_ARGS = ['AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'S3_BUCKET']

def s3_file_list(args) -> list:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.AWS_ACCESS_KEY,
        aws_secret_access_key=args.AWS_SECRET_KEY,
    )
    object_list = s3_client.list_objects(Bucket=args.S3_BUCKET)
    file_list = [file.get('Key') for file in object_list.get('Contents', []) if file.get('Size') > 0]
    # We get at most 1000 results at a time
    while len(object_list.get('Contents', []))==1000:
        print("Over 1000 found, getting next 1000")
        marker = object_list['Contents'][999].get('Key')
        print(f"Marker: {marker}")
        object_list = s3_client.list_objects(Bucket=args.S3_BUCKET, Marker=marker)
        print(f"Found {len(object_list)} more objects")
        file_list = file_list + [file.get('Key') for file in object_list.get('Contents', []) if file.get('Size') > 0]
    return file_list


if __name__ == '__main__':
    run_from_cli(s3_file_list, DESCRIPTION, ARG_DEFINITIONS, REQUIRED_ARGS)
