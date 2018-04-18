#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#

BINDIR=.venv/bin
USERNAME=${USER}

pip:clean test build
	sh prod-twine.sh

test_build:
	sh test-twine.sh

test:
	${BINDIR}/python setup.py test

build:
	${BINDIR}/python setup.py sdist
clean:
	rm -rf dist bdist sdist

init:
	${BINDIR}/pip install twine pymongo
	${BINDIR}/pip install --upgrade pip
	${BINDIR}/pip install keyring
	${BINDIR}/keyring set https://test.pypi.org/legacy/ ${USERNAME}
	${BINDIR}/keyring set https://upload.pypi.org/legacy/ ${USERNAME}