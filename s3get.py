#!/usr/bin/env python 

import sys
import os
#import shutil
import boto.s3.connection
import boto.s3.key

def usage():
    print >>sys.stderr, "s3get bucket key [localoutputfile]"
    print >>sys.stderr, "get an S3 object from an S3 bucket with a given key into a localoutputfile or stdout"
    return 1

verbose = 0

def main():
    global verbose

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        myargs.append(arg)

    if len(myargs) < 2:
        return usage()

    newfile = None
    try:
        s3 = boto.s3.connection.S3Connection()
        if verbose: print s3        

        bucketname = myargs[0]
        bucket = s3.get_bucket(bucketname)
        if verbose: print bucket

        k = boto.s3.key.Key(bucket)
        k.key = myargs[1]
        if len(myargs) >= 3:
            newfile = myargs[2]
            r = k.get_contents_to_filename(newfile)
            if verbose: print r
            if verbose: print k.metadata

            # verify size
            st = os.stat(newfile)
            if verbose: print 'check size', newfile, st.st_size, k.size
            if st.st_size != k.size:
                raise 'size'

            # verify md5
            md5 = k.md5
            with open(newfile, 'rb') as f:
                local_md5, local_md5_64 = k.compute_md5(f)
            user_md5 = k.get_metadata('user-md5')
            if user_md5 is None:
                if verbose: print 'check md5 local_md5', md5, local_md5
                if md5 != local_md5:
                    raise 'md5'
            else:
                if verbose: print 'check user_md5 local_md5', user_md5, local_md5
                if user_md5 != local_md5:
                    raise 'md5'
        else:
            r = k.get_contents_to_file(sys.stdout)
            if verbose: print r
    except:
        print >>sys.stderr, sys.exc_info()
        if newfile is not None:
            print >>sys.stderr, "unlink", newfile
            os.unlink(newfile)
        return 1
    
    return 0

sys.exit(main())
