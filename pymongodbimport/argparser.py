'''
Created on 12 Aug 2017

@author: jdrumgoole
'''

import argparse

def pymongodb_arg_parser( parents=None):
    '''
    Construct parser for pymongodbimport return it as a list suitable for passing to the parents
    argument of the next parser
    ''' 
    if parents is None:
        parents = []  
    parser = argparse.ArgumentParser( parents=parents, add_help=False )
    parser.add_argument( '--database', default="test", help='specify the database name [default: %(default)s]')
    parser.add_argument( '--collection', default="test", help='specify the collection name [default: %(default)s]')
    parser.add_argument( '--host', default="mongodb://localhost:27017/test", help='mongodb URI. see https://docs.mongodb.com/manual/reference/connection-string/ for details [default: %(default)s]')
    parser.add_argument( '--batchsize', type=int, default=500, help='set batch size for bulk inserts [default: %(default)s]' )
    parser.add_argument( '--restart', default=False, action="store_true", help="use record count insert to restart at last write also enable restart logfile [default: %(default)s]")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading [default: %(default)s]" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( "--fieldfile", default= None, type=str,  help="Field and type mappings")
    parser.add_argument( "--delimiter", default=",", type=str, help="The delimiter string used to split fields [default: %(default)s]")
    parser.add_argument( "filenames", nargs="*", help='list of files')
    #parser.add_argument('--version', action='version', version='%(prog)s version:' + version )
    parser.add_argument('--addfilename', default=False, action="store_true", help="Add file name field to every entry" )
    parser.add_argument('--addtimestamp', default="none", choices=[ "none", "now", "gen" ], help="Add a timestamp to each record [default: %(default)s]" )
    parser.add_argument('--hasheader',  default=False, action="store_true", help="Use header line for column names [default: %(default)s]")
    parser.add_argument( '--genfieldfile', default=False, action="store_true", help="Generate a fieldfile from the data file, we set hasheader to true [default: %(default)s]")
    parser.add_argument( '--id', default="mongodb", choices=[ "mongodb", "gen"], help="Autogenerate ID default [ %(default)s ]")
    parser.add_argument( '--onerror', default="warn", choices=[ 'fail', "warn" , "ignore"], help="What to do when we hit an error parsing a csv file [default: %(default)s]")
    
    parents.append( parser )
    return parents
    