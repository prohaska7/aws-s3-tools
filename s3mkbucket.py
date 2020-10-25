#!/usr/bin/env python3

def usage():
    print("s3mkbucket [--verbose] bucket...", file=sys.stderr)
    print("make s3 buckets", file=sys.stderr)
    return 1;

import sys
import boto3

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

    r = 0
    try:
        s3 = boto3.resource('s3')
        if verbose: print(s3)

        for bucketname in myargs:
            b = s3.create_bucket(ACL='private', Bucket=bucketname)
            if verbose: print(b)
    except:
        print(sys.exc_info(), file=sys.stderr)
        r = 1

    return r

if __name__ == '__main__':
    sys.exit(main())
