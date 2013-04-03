#!/usr/bin/env python 

import sys
import re
import boto.s3.connection
import boto.s3.key

def usage():
    print >>sys.stderr, "s3perm bucket [key...]"
    print >>sys.stderr, "[--perm=PERM] [--email=EMAIL,...] [--user=USER,...]"
    return 1

def main():
    verbose = 0
    perm = 'public-read'
    emails = []
    users = []

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        match = re.match("--perm=(.*)", arg)
        if match:
            perm = match.group(1)
            continue
        match = re.match("--email=(.*)", arg)
        if match:
            fields = match.group(1);
            for f in fields.split(','):
                emails.append(f)
            continue
        match = re.match("--user=(.*)", arg)
        if match:
            fields = match.group(1);
            for f in fields.split(','):
                users.append(f)
            continue
                                    
        myargs.append(arg)

    if len(myargs) < 1:
        return usage()

    s3 = boto.s3.connection.S3Connection()
    if verbose:
        print s3

    b = s3.get_bucket(myargs.pop(0))
    if verbose:
        print b

    if len(myargs) == 0:
        b.set_acl(perm)
    else:
        for u in users:
            for key in myargs:
                k = b.get_key(key)
                if verbose: print k, perm, u
                k.add_user_grant(perm, u)
        for u in emails:
            for key in myargs:
                k = b.get_key(key)
                if verbose: print k, perm, u        
                k.add_email_grant(perm, u)
        if len(users) == 0 and len(emails) == 0:
            for key in myargs:
                b.set_acl(perm, key)
    
    return 0

sys.exit(main())
