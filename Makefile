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

test_scripts:
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_import_main.py -h > /dev/null)
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_import_main.py --delimiter '|' data/10k.txt &> /dev/null)
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_multiimport_main.py -h > /dev/null)
	(export PYTHONPATH=`pwd` && python pymongo_import/pwc.py -h > /dev/null)
	(export PYTHONPATH=`pwd` && python pymongo_import/split_file.py -h > /dev/null)
test_all: test_scripts
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