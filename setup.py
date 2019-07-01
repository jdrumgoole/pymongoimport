"""
Created on 25 Jul 2017

1.8b1   : 07-May-2018 : Fixed bug in splitfile. Fixed tests.
1.7b3   : 07-May-2018 : Use strptime in preferennce to parse which gives a massive boost in performance.
1.7a2   : 06-May-2018 : Refactored commands into a command class. Removed the mapping for multiprocessing.
                        Changed the default forking model to "spawn" which dramatically improved performance.
1.6a6   : 03-May-2018 : Bug fix
1.6a4   : 02-May-2018 : Added locator field for files
1.6a3   : 01-May-2018 : Allow processing to continue when records are corrupt if onerror setting allow
1.5a8   : 30-Apr-2018 : For multi-processing disable hasheader as an arg if we are splitting files
1.5a3   : 29-Apr-2018 : Fixed field file name generation to work with autosplit files (hack)
1.5a2   : 29-Apr-2018 : Added additional audit data.
1.4.9a5 : 27-Apr-2018 : Now allow csv files with trailing empty fields that mean their are more fields than header line columns
1.4.9a4 : 23-Ape-2018 : Fixed stats reporting for per second record updates
1.4.9a3 : 23-Apr-2018 : Added a multiprocessing processing pool via --poolsize
1.4.9a1 : 22-Apr-2018 : Fixed typo in setup.py that stopped mongo_import running
1.4.8a9 : 19-Apr-2018 : Renamed binaries to prevent class with package name
1.4.8a8 : 19-Apr-2017 : Added --info argument to allow insertion of a string during auditing.
Version 1.4.7 : 8-Apr-2018 : Now only supports python 3.6.
Version 1.5.0 : 8-Mar-2019 : Fixed tests. New package for upload.
Version 1.5.1 : 8-Arp-2019 : Added --delimiter tab arg to allow tsv files with tab delimiters
@author: jdrumgoole
"""

from setuptools import setup, find_packages
import os
import glob

pyfiles = [f for f in os.listdir(".") if f.endswith(".py")]

setup(
    name="pymongoimport",
    version="1.5.2",

    author="Joe Drumgoole",
    author_email="joe@joedrumgoole.com",
    description="pymongoimport - a program for reading CSV files into mongodb",
    long_description=
    '''
Pymongo_import is a program that can parse a csv file from its header and first line to
create an automated type conversion file (a .ff file) to control how types in the CSV
file are converted. This file can be edited once created (it is a ConfigParser format file).
For types that fail conversion the type conversion will fail back on string conversion.
Blank columns in the CSV file are marked as [blank-0], [blank-1] ... [blank-n] and are ignored
by the parser.
''',

    license="AGPL",
    keywords="MongoDB import csv",
    url="https://github.com/jdrumgoole/pymongo_import",

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Affero General Public License v3',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6'],

    install_requires=["pymongo",
                      "nose",
                      "dnspython",
                      "dateutils"],

    packages=find_packages(),

    data_files=[("test", glob.glob("data/*.ff") +
                 glob.glob("data/*.csv") +
                 glob.glob("data/*.txt"))],
    python_requires='>3.6',
    scripts=[],
    entry_points={
        'console_scripts': [
            'pymongoimport=pymongoimport.pymongoimport_main:mongoimport_main',
            'splitfile=pymongoimport.splitfile:splitfile',
            'pymultiimport=pymongoimport.pymongomultiimport_main:multi_import',
            'pwc=pymongoimport.pwc:pwc',
        ]
    },

    test_suite='nose.collector',
    tests_require=['nose'],
)
