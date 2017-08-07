# pymongodbimport

## Usage:

pymongodbimport is a python program that will import data into a mongodb
database (default 'test' ) and a mongodb collection (default 'test' ).
 
Why do we have pymongodbimport when MongoDB has a perfectly good (and much faster)
[mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) program 
that is available for free in the standard MongoDB [community download](https://www.mongodb.com/download-center#community).

Well pymonogodbimport does a few things that mongoimport doesn't do (yet). For people
with new CSV fields there is the [**--genfieldfile** option]( #genfieldfile )

Each file in the input list must correspond to a fieldfile format that is
common across all the files. The fieldfile is specified by the  `--fieldfile` parameter.

An example run:

```
   $python pymongodbimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
```

## Arguments

### Positional arguments:
  *filenames*        : list of files to be processed

### Optional arguments:

**-h --help**      : Show the help message and exit.

**--database** *name* 

Specify the *name* of the database to use  [default: *test*]

**--collection** *name*

Specify the *name* of the collection to use [default : *test*]

**--host** *mongodb URI*
Specify the URI for connecting to the database. The full connection
URI syntax is documented on the
[MongoDB docs website.](https://docs.mongodb.com/manual/reference/connection-string/)

The default is *mongodb://localhost:27017/test*

**--batchsize** *batchsize*

set batch size for bulk inserts. This is the amount of docs the client
will add to a batch before trying to upload the whole chunk to the
server (reduces server round trips). The default *batchsize* is 500.

For larger documents you may find a smaller *batchsize* is more efficient.

**--restart**

For large batches you may want to restart the batch if uploading is
interrupted. Restarts are stored in the current database in a collection 
called *restartlog*. Each file to be uploaded has its own record in the 
*restartlog*. The restart log record format is :

```
{ "name"           : <name of file being uploaded>, 
  "timestamp"      : <datetime that this doc was inserted>,
  "batch_size"     : <the batchsize specified by --batchsize>,
  "count"          : <the total number of documents inserted from <name> file to <timestamp> >,
  "doc_id"         : <The mongodb _id field for the last record inserted in this batch> }
```

The restart log is keyed of the filename so each filename must be unique otherwise
imports that are running in parallel will overwrite each others restart logs.
use record count insert to restart at last write also
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

  <a name="genfieldfile"></a>**--genfieldfile**        
  
  Generate a fieldfile from the data file, we set
                        hasheader to true [default: False]

  --id {mongodb,gen}    Autogenerate ID default [ mongodb ]

  --onerror {fail,warn,ignore}
                        What to do when we hit an error parsing a csv file
                        [default: warn]
