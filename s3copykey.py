#!/usr/bin/env python 

import sys
import boto.s3.connection

def usage():
    print >>sys.stderr, "s3copykey [destbucket] [destkey] [srcbucket] [srckey]"
    print >>sys.stderr, "copy a key from one bucket to another bucket"
    return 1

def main():
    verbose = 0
    
    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "--help" or arg == "-?":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        myargs.append(arg)

    if len(myargs) != 4:
        return 1

    s3 = boto.s3.connection.S3Connection()
    if verbose:
        print s3

    destbucket = s3.get_bucket(myargs[0])
    if verbose:
        print destbucket

    r = destbucket.copy_key(myargs[1], myargs[2], myargs[3])
    if verbose:
        print r

    return 0

sys.exit(main())
