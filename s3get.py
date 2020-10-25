#!/usr/bin/env python3

import sys
import os
import hashlib
import string
import boto3
import traceback
import logging

def usage():
    print("s3get bucket key [localoutputfile]", file=sys.stderr)
    print("get an S3 object from an S3 bucket with a given key into a localoutputfile or stdout", file=sys.stderr)
    return 1

def main():
    verbose = 0
    ignore_md5 = 0
    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        if arg == '--ignore-md5':
            ignore_md5 = 1
        myargs.append(arg)

    if len(myargs) < 2:
        return usage()

    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    newfile = None
    try:
        s3 = boto3.resource('s3')
        if verbose: print(s3)

        bucketname = myargs[0]
        keyname = myargs[1]

        # convert keyname to unicode
        try:
            keyname = keyname.decode('utf-8')
        except:
            pass

        obj = s3.Object(bucketname, keyname)
        if verbose: print(obj)

        if len(myargs) >= 3:
            # get the object and store into a local file
            newfile = myargs[2]
            with open(newfile, 'wb') as f:
                obj.download_fileobj(f)

            # verify size
            st = os.stat(newfile)
            if verbose: print('check size', newfile, st.st_size, obj.content_length)
            if st.st_size != obj.content_length:
                raise ValueError('size')

            # compute local md5
            local_md5 = compute_md5(newfile)
            
            # verify md5
            md5 = obj.e_tag.strip('"')
            if verbose: print(local_md5, md5)
            if local_md5 != md5:
                if not 'use-md5' in obj.metadata:
                    if not ignore_md5:
                        e = 'user-md5 metadata missing local_md5=%s etag=%s' % (local_md5, md5)
                        raise ValueError(e)

            if 'user-md5' in obj.metadata:
                user_md5 = obj.metadata['user-md5']
                if verbose: print(local_md5, user_md5)
                if local_md5 != user_md5:
                    if not ignore_md5:
                        e = 'user md5 different local_md5=%s user_md5=%s' % (local_md5, user_md5)
                        raise ValueError(e)
        else:
            obj.download_fileobj(sys.stdout)
    except:
        e = sys.exc_info()
        print(e, file=sys.stderr)
        traceback.print_tb(e[2])
        if newfile is not None:
            print("unlink", newfile, file=sys.stderr)
            try:
                os.unlink(newfile)
            except:
                print(sys.exc_info(), file=sys.stderr)
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
