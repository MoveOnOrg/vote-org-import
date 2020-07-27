from pywell.entry_points import run_from_cli, run_from_lamba
from pywell.get_psql_results import get_psql_results

from s3_file_list import s3_file_list

DESCRIPTION = 'Import any new files from vote.org.'

ARG_DEFINITIONS = {
    'AWS_ACCESS_KEY': 'AWS IAM key.',
    'AWS_SECRET_KEY': 'AWS IAM secret.',
    'S3_BUCKET': 'Name of bucket to search.',
    'S3_BUCKET_REGION': 'Region of S3 bucket.',
    'DB_HOST': 'Database host IP or hostname.',
    'DB_PORT': 'Database port number.',
    'DB_USER': 'Database user.',
    'DB_PASS': 'Database password.',
    'DB_NAME': 'Database name.',

}

REQUIRED_ARGS = [
    'AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'S3_BUCKET',
    'DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASS', 'DB_NAME'
]

def import_new_files(args):
    args.DB_QUERY = """
    SELECT last_run
    FROM tech.script_last_run
    WHERE script = 'vote-org-import'
    """
    last_run = get_psql_results(args)[0].get('last_run')
    last_run_file = last_run.strftime('%Y-%m-%d-%H-%M-00')
    files = [file.split('/') for file in s3_file_list(args) if file.split('/')[0] > last_run_file]
    files.sort(key=lambda file:file[0])
    args.NO_RESULTS = True
    if len(files):
        for file in files:
            args.DB_QUERY = """
            COPY tao.vote_dot_org_events
            FROM 's3://%s/%s/%s'
            region AS '%s'
            csv acceptinvchars
            access_key_id '%s'
            secret_access_key '%s';
            """ % (
                args.S3_BUCKET,
                file[0],
                file[1],
                args.S3_BUCKET_REGION,
                args.AWS_ACCESS_KEY,
                args.AWS_SECRET_KEY
            )
            print('importing %s/%s ...' % (file[0], file[1]))
            get_psql_results(args)
        new_last_run_parts = files[-1][0].split('-')
        new_last_run = '%s %s' % ('-'.join(new_last_run_parts[0:3]), ':'.join(new_last_run_parts[3:6]))
        args.DB_QUERY = """
        UPDATE tech.script_last_run
        SET last_run = '%s'
        WHERE script = 'vote-org-import'
        """ % new_last_run
        get_psql_results(args)
    else:
        new_last_run = last_run.strftime('%Y-%m-%d %H:%M:00')
    return {'imports': len(files), 'last': new_last_run}


def aws_lambda(event, context):
    return run_from_lamba(import_new_files, DESCRIPTION, ARG_DEFINITIONS, REQUIRED_ARGS, event)


if __name__ == '__main__':
    run_from_cli(import_new_files, DESCRIPTION, ARG_DEFINITIONS, REQUIRED_ARGS)