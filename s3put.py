#!/usr/bin/env python 

import sys
import os
import hashlib
import boto3

def usage():
    print >>sys.stderr, "s3put bucket key [localinputfile]"
    print >>sys.stderr, "put an object in an S3 bucket with a given key and content from a localinputfile or stdin"
    print >>sys.stderr, "[--verbose]"
    return 1

def main():
    verbose = 0

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1; continue
        myargs.append(arg)

    if len(myargs) < 2:
        return usage()
    try:
        s3 = boto3.resource('s3')
        if verbose: print s3

        bucketname = myargs[0]
        keyname = myargs[1]

        obj = s3.Object(bucketname, keyname)
        if verbose: print obj

        if len(myargs) >= 3:
            localfile = myargs[2]
            
            # compute md5
            local_md5 = compute_md5(localfile)

            # set key value from filename and use precomputed md5
            with open(localfile, 'rb') as f:
                obj.upload_fileobj(f, ExtraArgs={ 'Metadata': { 'user-md5': local_md5 } })
        else:
            obj.upload_fileobj(sys.stdin)
        if verbose: print r
    except:
        print >>sys.stderr, sys.exc_info()
        return 1
    
    return 0

def compute_md5(file):
    md5 = hashlib.md5()
    with open(file, 'rb') as f:
        n = 1024*1024
        b = f.read(n)
        while b:
            md5.update(b)
            b = f.read(n)
    return md5.hexdigest()

sys.exit(main())
