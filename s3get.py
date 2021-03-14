#!/usr/bin/env python3

import sys
import os
import hashlib
import string
import boto3
import traceback
import logging

def usage():
    print("s3get [-v] bucket key file", file=sys.stderr)
    print("get an S3 object from an S3 bucket with a given key into a file", file=sys.stderr)
    print("s3get [-r] [-v] bucket keyprefix", file=sys.stderr)
    return 1

def main():
    verbose = False
    logdebug = False
    ignore_md5 = False
    recursive = False
    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = True
            continue
        if arg == '--logdebug':
            logdebug = True
            contine
        if arg == '-r' or arg == '--recursive':
            recursive = True
            continue
        if arg == '--ignore-md5':
            ignore_md5 = True
            continue
        myargs.append(arg)

    if logdebug:
        logging.basicConfig(level=logging.DEBUG)

    try:
        s3 = boto3.resource('s3')
        if verbose: print(s3)

        if recursive:
            if len(myargs) != 2:
                return usage()

            bucketname = myargs[0]
            keyprefix = myargs[1]

            bucket = s3.Bucket(bucketname)
            if verbose: print(bucket)

            for k in bucket.objects.filter(Prefix=keyprefix):
                get(bucket, k.key, k.key, verbose)
        else:
            if len(myargs) != 3:
                return usage()

            bucketname = myargs[0]
            keyname = myargs[1]
            newfile = myargs[2]

            bucket = s3.Bucket(bucketname)
            if verbose: print(bucket)

            get(bucket, keyname, newfile, verbose)
    except:
        e = sys.exc_info()
        print(e, file=sys.stderr)
        traceback.print_tb(e[2])
        return 1
    
    return 0

def get(bucket, keyname, newfile, verbose):
    try:
        # convert keyname to unicode
        try:
            keyname = keyname.decode('utf-8')
        except:
            pass

        obj = bucket.Object(keyname)
        if verbose: print(obj)

        newfiledir = os.path.dirname(newfile)
        if newfiledir != '':
            os.makedirs(newfiledir, exist_ok=True)
        
        # get the object and store into a local file
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
        if verbose: print('e_tag', local_md5, md5)
        if local_md5 != md5:
            if not 'user-md5' in obj.metadata:
                if not ignore_md5:
                    e = 'user-md5 metadata missing local_md5=%s etag=%s' % (local_md5, md5)
                    raise ValueError(e)

        if 'user-md5' in obj.metadata:
            user_md5 = obj.metadata['user-md5']
            if verbose: print('user-md5', local_md5, user_md5)
            if local_md5 != user_md5:
                if not ignore_md5:
                    e = 'user md5 different local_md5=%s user_md5=%s' % (local_md5, user_md5)
                    raise ValueError(e)
        else:
            print('WARN user-md5 missing', keyname)
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
        raise

def compute_md5(file):
    md5 = hashlib.md5()
    with open(file, 'rb') as f:
        n = 1024*1024
        b = f.read(n)
        while b:
            md5.update(b)
            b = f.read(n)
    return md5.hexdigest()

if __name__ == '__main__':
    sys.exit(main())
