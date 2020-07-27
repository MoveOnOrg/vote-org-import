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
    file_list = [file.get('Key') for file in object_list.get('Contents', [])]
    return file_list


if __name__ == '__main__':
    run_from_cli(s3_file_list, DESCRIPTION, ARG_DEFINITIONS, REQUIRED_ARGS)
