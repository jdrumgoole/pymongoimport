#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#


PYPIUSERNAME="jdrumgoole"
ROOT=${HOME}/GIT/pymongoimport

root:
	@echo "The project ROOT is '${ROOT}'"


python_bin:
	python -c "import os;print(os.environ.get('USERNAME'))"
	which python

prod_build:clean build test
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/* -u jdrumgoole

test_build:
	twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u jdrumgoole

#
# Just test that these scripts run
#
test_scripts:
	(export PYTHONPATH=`pwd` && python pymongoimport/pymongoimport_main.py -h > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongoimport/pymongoimport_main.py --delimiter '|' data/10k.txt > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongoimport/pymongomultiimport_main.py -h > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongoimport/pwc.py -h > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongoimport/splitfile.py -h > /dev/null 2>&1)

test_data:
	(export PYTHONPATH=`pwd` && python pymongoimport/splitfile.py --hasheader --autosplit 4 data/yellow_tripdata_2015-01-06-200k.csv > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongoimport/pymongo_multiimport_main.py --poolsize 2 yellow_tripdata_2015-01-06-200k.csv.[12] > /dev/null 2>&1)
	(rm yellow_tripdata_2015-01-06-200k.csv.*)
	(export PYTHONPATH=`pwd` && python pymongoimport/splitfile.py --autosplit 4 data/100k.txt > /dev/null 2>&1)
	(export PYTHONPATH=`pwd` && python pymongoimport/pymongomultiimport_main.py --delimiter "|" --poolsize 2 100k.txt.[12] > /dev/null 2>&1)
	(rm 100k.txt.* > /dev/null 2>&1)

test_all: test_scripts
	python setup.py test

nose:
	which python
	nosetests

dist:
	python setup.py sdist

clean:
	rm -rf dist bdist sdist

pkgs:
	pipenv install pymongo keyring twine nose semvermanager

init: pkgs
	keyring set https://test.pypi.org/legacy/ ${USERNAME}
	keyring set https://upload.pypi.org/legacy/ ${USERNAME}

