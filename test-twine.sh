#!bin/sh
# uses key_ring to store password
.venv/bin/twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u jdrumgoole
