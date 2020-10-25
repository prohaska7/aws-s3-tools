#!/usr/bin/env python3

import sys
import re
import boto3

def usage():
    print("s3url [--expires=seconds] bucket key", file=sys.stderr)
    print("generate the url for an s3 object", file=sys.stderr)
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

    c = boto3.client('s3')
    if verbose:
        print(c)

    r = c.generate_presigned_post(myargs[0], myargs[1], ExpiresIn=expires)
    print(r)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
