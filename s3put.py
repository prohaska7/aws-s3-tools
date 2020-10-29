#!/usr/bin/env python3

import sys
import os
import hashlib
import boto3
import traceback
import logging

def usage():
    print("s3put [--verbose|-v] bucket key localinputfile", file=sys.stderr)
    print("put an object in an S3 bucket with a given key and content from a localinputfile or stdin", file=sys.stderr)
    print("s3put [--recursive|-r] [--verbose|-v] bucket directory", file=sys.stderr)
    print("recursive put all files into an S3 bucket", file=sys.stderr)
    return 1

def main():
    verbose = False
    recursive = False
    debug = False
    ignore = False

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = True; continue
        if arg == "-r" or arg == "--recursive":
            recursive = True; continue
        if arg == "-d":
            debug = True; continue
        if arg == "-i":
            ignore = True; continue
        myargs.append(arg)

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    try:
        s3 = boto3.resource('s3')
        if verbose: print(s3)

        if recursive:
            if len(myargs) != 2:
                return usage()
            put_dir(s3, myargs[0], myargs[1], verbose, ignore)
        else:
            if len(myargs) != 3:
                return usage()
            put_object(s3, myargs[0], myargs[1], myargs[2], verbose, ignore)
    except:
        e = sys.exc_info()
        print(e, file=sys.stderr)
        traceback.print_tb(e[2])
        return 1
    return 0

def put_dir(s3, bucketname, dirname, verbose, ignore):
    for path,dirs,files in os.walk(dirname):
        for fname in files:
            pathfile = path + '/' + fname
            put_object(s3, bucketname, pathfile, pathfile, verbose, ignore)

def put_object(s3, bucketname, keyname, localfile, verbose, ignore):
    obj = s3.Object(bucketname, keyname)
    if verbose: print(obj)

    # compute md5
    local_md5 = compute_md5(localfile)

    # set key value from filename and use precomputed md5
    with open(localfile, 'rb') as f:
        config = None
        if verbose:
            config = boto3.s3.transfer.TransferConfig(use_threads=False)
        try:
            obj.upload_fileobj(f, ExtraArgs={ 'Metadata': { 'user-md5': local_md5 } }, Config=config)
        except:
            if ignore:
                e = sys.exc_info()
                print('IGNORE upload', e)
            else:
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

sys.exit(main())
