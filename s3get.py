#!/usr/bin/env python 

import sys
import os
import shutil
import boto.s3.connection
import boto.s3.key

def usage():
    print >>sys.stderr, "s3get bucket key [localoutputfile]"
    print >>sys.stderr, "get an S3 object from an S3 bucket with a given key into a localoutputfile or stdout"
    print >>sys.stderr, "s3get --bundle bucket file"
    print >>sys.stderr, "get a bundled file from an S3 bucket"
    return 1

verbose = 0

def main():
    global verbose
    bundle = 0

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
        if arg == "--bundle":
            bundle = 1
            continue
        myargs.append(arg)

    if bundle:
        if len(myargs) != 2:
            return usage()
        dname = None
        try:
            s3 = boto.s3.connection.S3Connection()
            if verbose: print s3
            bucketname = myargs[0]
            bucket = s3.get_bucket(bucketname)
            if verbose: print bucket
            # make a temp dir
            origdir = os.getcwd()
            dname = ".s3tmp.%d" % (os.getpid())
            if verbose: print dname
            get_big_file(bucket, bucketname, myargs[1], dname)
        except:
            print >>sys.stderr, sys.exc_info()
            # cleanup the tmp dir
            if dname is not None:
                print >>sys.stderr, "cleanup", origdir + '/' + dname
                shutil.rmtree(origdir + '/' + dname)
            return 1
    else:
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
                r = k.get_contents_to_filename(myargs[2])
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

import xml.sax

class bigfile_content_handler(xml.sax.ContentHandler):
    def __init__(self):
        self.chunks = []
        self.name = None

    def startElement(self, name, attrs):
        for n in attrs.getNames():
            v = attrs.getValue(n)
            setattr(self, n, v)
        
    def endElement(self, name):
        if name == "chunk":
            n = self.name
            if n is None:
                n = self.ch
            self.chunks.append(n)
            self.name = None
        else:
            setattr(self, name, self.ch)
        self.ch = None

    def characters(self, ch):
        self.ch = ch

def get_big_file(bucket, bucketname, key, dname):
    os.mkdir(dname)
    os.chdir(dname)

    # get the bundle descriptor
    xmlfilename = key + ".xml"
    get_file(bucket, bucketname, xmlfilename, xmlfilename)

    # parse the bundle descriptor
    bigfilehandler = bigfile_content_handler()
    xml.sax.parse(xmlfilename, bigfilehandler)
    big_filename = bigfilehandler.filename
    expectmd5 = bigfilehandler.md5
    chunks = bigfilehandler.chunks
    if verbose: print big_filename, expectmd5, chunks

    # get the bundle files
    for chunk in chunks:
        get_file(bucket, bucketname, chunk, chunk)

    # concat the bundle files
    if len(chunks) == 1:
        os.link(chunks[0], "../" + big_filename)
    else:
        concat(bigfilehandler.chunks, "../" + big_filename)

    os.chdir("..")

    # verify the md5 sum
    cmd = "md5sum " + big_filename
    if verbose: print cmd
    md5file = os.popen(cmd)
    md5 = md5file.read().split()
    if verbose: print md5
    assert len(md5) == 2 and md5[1] == big_filename
    md5 = md5[0]
    md5file.close()
    assert expectmd5 == md5

    # cleanup
    for chunk in chunks:
        os.unlink(dname + "/" + chunk)
    os.unlink(dname + "/" + xmlfilename)
    os.rmdir(dname)

def get_file(bucket, bucketname, key, file):
    k = boto.s3.key.Key(bucket)
    k.key = key
    if verbose: print "get " + bucketname + " " + key
    r = k.get_contents_to_filename(file)
    if verbose: print r

def concat(files, new_filename):
    cmd = "cat " + " ".join(files) + " >" + new_filename
    if verbose: print cmd
    os.system(cmd)

sys.exit(main())
