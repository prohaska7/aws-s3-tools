#!/usr/bin/env python 

import sys
import boto.s3.connection
import boto.s3.key

def usage():
    print >>sys.stderr, "s3put bucket key [localinputfile]"
    print >>sys.stderr, "put an object in an S3 bucket with a given key and content from a localinputfile or stdin"
    print >>sys.stderr, "[--verbose]"
    return 1

verbose = 0

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
        s3 = boto.s3.connection.S3Connection()
        if verbose: print s3
        bucketname = myargs[0]
        keyname = myargs[1]
        bucket = s3.get_bucket(bucketname)
        if verbose: print bucket
        k = boto.s3.key.Key(bucket)
        k.key = keyname
        if len(myargs) >= 3:
            # compute md5
            with open(myargs[2], 'rb') as f:
                user_md5, user_md5_64 = k.compute_md5(f)

            # set md5 metadata
            k.set_metadata('user-md5', user_md5)
            if verbose: print k.metadata

            # set key value from filename and use precomputed md5
            r = k.set_contents_from_filename(myargs[2], md5=(user_md5, user_md5_64))
            if verbose: print k.metadata
        else:
            r = k.set_contents_from_file(sys.stdin)
        if verbose: print r
    except:
        print >>sys.stderr, sys.exc_info()
        return 1
    
    return 0

sys.exit(main())
