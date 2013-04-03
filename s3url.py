#!/usr/bin/env python 

import sys
import re
import boto.s3.connection
import boto.s3.key

def usage():
    print >>sys.stderr, "s3url [--expires=seconds] bucket key"
    print >>sys.stderr, "generate the url for an s3 object"
    return 1

def main():
    verbose = 0
    expires = 52*7*24*60*60

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        match = re.match("--expires=(.*)", arg)
        if match:
            expires = match.group(1)
            continue
        myargs.append(arg)

    if len(myargs) != 2:
        return usage()

    s3 = boto.s3.connection.S3Connection()
    if verbose:
        print s3

    b = s3.get_bucket(myargs[0])
    if verbose:
        print b

    k = boto.s3.key.Key(b)
    k.key = myargs[1]
    print k.generate_url(expires)
    
    return 0

sys.exit(main())
