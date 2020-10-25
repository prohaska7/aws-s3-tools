#!/usr/bin/env python3

import sys
import re
import boto3
import traceback

def usage():
    print("s3rm [--verbose] [-r] bucket [key...]", file=sys.stderr)
    print("delete a bucket or", file=sys.stderr)
    print("delete keys from a bucket", file=sys.stderr)
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
    if verbose: print(bucketname)

    s3 = boto3.resource('s3')
    if verbose: print(s3)
    bucket = s3.Bucket(bucketname)
    if verbose: print(bucket)

    exitr = 0
    if len(myargs) == 0:
        if rflag:
            for obj in bucket.objects.all():
                resp = obj.delete()
                if verbose: print(resp)
        try:
            resp = bucket.delete()
            if verbose: print(resp)
        except:
            e = sys.exc_info()
            print(e, file=sys.stderr)
            traceback.print_tb(e[2])
            exitr = 1
    else:
        for keyname in myargs:
            if verbose: print("remove key", keyname)
            deleted = 0
            for obj in bucket.objects.filter(Prefix=keyname):
                resp = obj.delete()
                if verbose: print(resp)
                deleted += 1
            if deleted == 0:
                print('could not rm', keyname, file=sys.stderr)
                exitr = 1

    return exitr

if __name__ == '__main__':
    sys.exit(main())
