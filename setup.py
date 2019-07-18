

from setuptools import setup, find_packages
import os
import glob

pyfiles = [f for f in os.listdir(".") if f.endswith(".py")]

setup(
    name="pymongoimport",
    version="1.5.6",

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
    keywords="MongoDB import csv tsv",
    url="https://github.com/jdrumgoole/pymongoimport",

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
        'Programming Language :: Python :: 3.7'],

    install_requires=["pymongo",
                      "nose",
                      "dnspython",
                      "dateutils",
                      "toml"],

    packages=find_packages(),

    data_files=[("test", glob.glob("data/*.ff") +
                 glob.glob("data/*.csv") +
                 glob.glob("data/*.txt"))],
    python_requires='>3.7',
    scripts=[],
    entry_points={
        'console_scripts': [
            'pymongoimport=pymongoimport.pymongoimport_main:pymongoimport_main',
            'splitfile=pymongoimport.splitfile:split_file_main',
            'pymultiimport=pymongoimport.pymongomultiimport_main:multi_import',
            'pwc=pymongoimport.pwc:pwc',
        ]
    },

    test_suite='nose.collector',
    tests_require=['nose'],
)
