#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#


USERNAME=${USER}
ROOT=${HOME}/GIT/pymongo_import

root:
	@echo "The project ROOT is '${ROOT}'"

bump_tag:
	semvermgr --bump tag_version setup.py

prod_build:clean test build
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/* -u jdrumgoole

test_build:
	twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u jdrumgoole

#
# Just test that these scripts run
#
test_scripts:
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_import_main.py -h > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_import_main.py --delimiter '|' data/10k.txt > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_multiimport_main.py -h > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongo_import/pwc.py -h > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongo_import/splitfile.py -h > /dev/null 2>&1)

test_data:
	(export PYTHONPATH=`pwd` && python pymongo_import/splitfile.py --hasheader --autosplit 4 data/yellow_tripdata_2015-01-06-200k.csv > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_multiimport_main.py --poolsize 2 yellow_tripdata_2015-01-06-200k.csv.[12] > /dev/null 2>&1)
	(rm yellow_tripdata_2015-01-06-200k.csv.*)
	(export PYTHONPATH=`pwd` && python pymongo_import/splitfile.py --autosplit 4 data/100k.txt > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongo_import/pymongo_multiimport_main.py --delimiter "|" --poolsize 2 100k.txt.[12] > /dev/null 2>&1)
	(rm 100k.txt.* > /dev/null 2>&1)

test_all: test_scripts
	python setup.py test

nose:
	nosetests

build:
	python setup.py sdist

clean:
	rm -rf dist bdist sdist

pkgs:
	pipenv install pymongo keyring twine nose semvermanager

init: pkgs
	keyring set https://test.pypi.org/legacy/ ${USERNAME}
	keyring set https://upload.pypi.org/legacy/ ${USERNAME}

