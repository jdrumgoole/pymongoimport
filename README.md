# pymongodbimport

A program to import data in CSV format. The program allows types for fields to be defined using a type file called a field file. The field file is stored in Python ConfigParser format with one section per field. 

Arguments:

`
optional arguments:
  -h, --help            show this help message and exit
  --database DATABASE   specify the database name
  --collection COLLECTION
                        specify the collection name
  --host HOST           hostname
  --port PORT           port name
  --username USERNAME   username to login to database
  --password PASSWORD   password to login to database
  --admindb ADMINDB     Admin database used for authentication
  --ssl                 use SSL for connections
  --chunksize CHUNKSIZE
                        set chunk size for bulk inserts
  --skip SKIP           skip lines before reading
  --restart             use record count to skip
  --insertmany          use insert_many
  --testlogin           test database login
  --drop                drop collection before loading
  --ordered             forced ordered inserts
  --verbose VERBOSE     controls how noisy the app is
  --fieldfile FIELDFILE
                        Field and type mappings
  --delimiter DELIMITER
                        The delimiter string used to split fields (default
                        '|')
  --testmode            Run in test mode, no updates
  --multi MULTI         Run in multiprocessing mode (experimental)
  --version             show program's version number and exit
  `