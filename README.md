# pymongodbimport

usage:

pymongodbimport is a python program that will import data into a mongodb
 database (default 'test' ) and a mongodb collection (default 'test' ).

Each file in the input list must correspond to a fieldfile format that is
common across all the files. The fieldfile is specified by the  `--fieldfile` parameter.

An example run:

```
   $python pymongodbimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
```

## Arguments

### positional arguments:
  *filenames*        : list of files to be processed

### xoptional arguments:

**-h --help**      : Show the help message and exit.

**--database <name>**

Specify the name of the database to use  [default: test]

**--collection <name**

Specify the name of the collection to use [default : test]
**--host <mongodb URI>**
Specify the URI for connecting to the database.  The format is defined by MongoDB see
mongodb://localhost:270017 : std URI arguments apply
                        [default: mongodb://localhost:27017/test]
  --chunksize CHUNKSIZE
                        set chunk size for bulk inserts[default: 500]
  --restart             use record count insert to restart at last write also
                        enable restart logfile [default: False]
  --drop                drop collection before loading [default: False]
  --ordered             forced ordered inserts
  --fieldfile FIELDFILE
                        Field and type mappings
  --delimiter DELIMITER
                        The delimiter string used to split fields [default: ,]
  --version             show program's version number and exit
  --addfilename         Add file name field to every entry
  --addtimestamp {none,now,gen}
                        Add a timestamp to each record [default: none]
  --hasheader           Use header line for column names [default: False]
  --genfieldfile        Generate a fieldfile from the data file, we set
                        hasheader to true [default: False]
  --id {mongodb,gen}    Autogenerate ID default [ mongodb ]
  --onerror {fail,warn,ignore}
                        What to do when we hit an error parsing a csv file
                        [default: warn]
JD10Gen:bin jdrumgoole$

