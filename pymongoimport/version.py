"""
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

"""
__VERSION__ = "1.8b1"  # type: str
