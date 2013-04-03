#!/usr/bin/env python 

import sys
import re
import boto.s3.connection

def usage():
    print >>sys.stderr, "s3rm [--verbose] [-r] bucket [key...]"
    print >>sys.stderr, "delete a bucket or"
    print >>sys.stderr, "delete keys from a bucket"
    return 1

def main():
    verbose = 0
    rflag = 0

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        if arg == "-r" or arg == "--recursive":
            rflag = 1
            continue
        myargs.append(arg)

    if len(myargs) == 0:
        return usage()

    bucketname = myargs.pop(0)
    if verbose:
        print bucketname

    s3 = boto.s3.connection.S3Connection()
    if verbose:
        print s3
    b = s3.get_bucket(bucketname)
    if verbose:
        print b

    if len(myargs) == 0:
        if rflag:
            keys = b.get_all_keys()
            for k in keys:
                b.delete_key(k.name)
                
        r = b.delete()
        if verbose:
            print r
    else:
        for keyname in myargs:
            if verbose:
                print "remove key", keyname
            r = b.delete_key(keyname)
            if verbose:
                print r

    return 0

sys.exit(main())
