#!/usr/bin/env python 

def usage():
    print >>sys.stderr, "s3mkbucket [--verbose] bucket..."
    print >>sys.stderr, "make s3 buckets"
    return 1;

import sys
import boto.s3.connection

def main():
    verbose = 0
    
    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "--help":
            return usage();
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        myargs.append(arg)

    s3 = boto.s3.connection.S3Connection()
    if verbose:
        print s3

    for bucketname in myargs:
        b = s3.create_bucket(bucketname)
        if verbose:
            print b

    return 0

sys.exit(main())
