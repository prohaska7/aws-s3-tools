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
        return usage()

    try:
        s3 = boto.s3.connection.S3Connection()
        if verbose:
            print s3
    except:
        print >>sys.stderr, "s3 connection", sys.exc_info()
        return 1;

    try:
        destbucket = s3.get_bucket(myargs[0])
        if verbose:
            print destbucket
    except:
        print >>sys.stderr, "get bucket", sys.exc_info()
        return 1

    try:
        r = destbucket.copy_key(myargs[1], myargs[2], myargs[3])
        if verbose:
            print r
    except:
        print >>sys.stderr, "copy key", sys.exc_info()
        return 1

    return 0

sys.exit(main())
