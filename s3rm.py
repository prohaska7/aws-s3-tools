#!/usr/bin/env python 

import sys
import re
import boto3
import traceback

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
    if verbose: print bucketname

    s3 = boto3.resource('s3')
    if verbose: print s3
    b = s3.Bucket(bucketname)
    if verbose: print b

    exitr = 0
    if len(myargs) == 0:
        if rflag:
            for key in b.objects.all():
                resp = key.delete()
                if verbose: print resp
        try:
            resp = b.delete()
            if verbose: print resp
        except:
            e = sys.exc_info()
            print >>sys.stderr, e
            traceback.print_tb(e[2])
            exitr = 1
    else:
        for keyname in myargs:
            if verbose: print "remove key", keyname
            deleted = 0
            for obj in b.objects.filter(Prefix=keyname):
                resp = obj.delete()
                if verbose: print resp
                deleted += 1
            if deleted == 0:
                print >>sys.stderr, 'could not rm', keyname
                exitr = 1

    return exitr

sys.exit(main())
