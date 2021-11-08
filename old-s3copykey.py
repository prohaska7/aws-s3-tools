#!/usr/bin/env python3

import sys
import traceback
import boto3

def usage():
    print("s3copykey [--verbose] destbucket destkey srcbucket srckey", file=sys.stderr)
    print("copy a key from one bucket to another bucket", file=sys.stderr)
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

    dest = { 'Bucket': myargs[0], 'Key': myargs[1] }
    src = { 'Bucket': myargs[2], 'Key': myargs[3] }
    try:
        s3 = boto3.resource('s3')
        if verbose:
            print(s3)
        s3.meta.client.copy(src, dest['Bucket'], dest['Key'])
    except:
        e = sys.exc_info()
        print(e, 'dest=', dest, 'src=', src, file=sys.stderr)
        traceback.print_tb(e[2])
        return 1;

    return 0

if __name__ == '__main__':
    sys.exit(main())
