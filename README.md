# Vote.org Import

This Python 3.6 connects to an S3 bucket of vote.org exports and imports any new files.

# Install

Run `pip install -r requirements.txt` to get requirements to run the code locally, and `pip install -r dev_requirements.txt` to get requirements for running tests (Pytest) and deploying code (Zappa). Copy `settings.py.template` to `settings.py` and fill in any credentials you don't want to pass as command line arguments.
