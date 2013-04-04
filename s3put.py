#!/usr/bin/env python 

import sys
import re
import os
import glob
import shutil
import boto.s3.connection
import boto.s3.key
from stat import *

def usage():
    print >>sys.stderr, "s3put bucket key [localinputfile]"
    print >>sys.stderr, "put an object in an S3 bucket with a given key and content from a localinputfile or stdin"
    print >>sys.stderr, "s3put --bundle bucket file"
    print >>sys.stderr, "put a big file into an amazon S3 bucket"
    print >>sys.stderr, "split the file into small chunks, compute the md5 sum of the file, generate an XML descriptor of the file, and upload to an S3 bucket"
    print >>sys.stderr, "[--verbose] [--debug]"
    print >>sys.stderr, "[--size=%s] the size of file chunks that the big file is split into" % (chunksizestr)
    return 1

verbose = 0
bundle = 0
split = 0
debug = 0
chunksizestr = "1024m"

def main():
    global verbose, bundle, split, debug, chunksizestr

    myargs = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1; continue
        if arg == "--debug":
            debug = 1; continue
        if arg == "--bundle":
            bundle = 1; continue
        if arg == "--split":
            split = 1; continue
        match = re.match("--size=(.*)", arg)
        if match:
            chunksizestr = match.group(1); continue
        myargs.append(arg)

    if bundle:
        if len(myargs) != 2:
            return usage()
        dname = None
        try:
            if debug:
                s3 = None
            else:
                s3 = boto.s3.connection.S3Connection()
                if verbose: print s3
            bucketname = myargs[0]
            filename = myargs[1]
            bucket = None
            if debug:
                bucket = None
            else:
                bucket = s3.get_bucket(bucketname)
                if verbose: print bucket
            # make a temp directory for the files to upload
            dname = ".s3tmp.%d" % (os.getpid())
            origdir = os.getcwd()
            if verbose: print dname
            put_big_file(bucket, bucketname, filename, dname)
        except:
            print >>sys.stderr, sys.exc_info()
            if dname is not None:
                print >>sys.stderr, "cleanup", origdir + '/' + dname
                shutil.rmtree(origdir + '/' + dname)
            return 1
    else:
        if len(myargs) < 2:
            return usage()
        newfile = None
        try:
            if debug:
                s3 = None
            else:
                s3 = boto.s3.connection.S3Connection()
                if verbose: print s3
            bucketname = myargs[0]
            keyname = myargs[1]
            bucket = s3.get_bucket(bucketname)
            if verbose: print bucket
            k = boto.s3.key.Key(bucket)
            k.key = keyname
            if len(myargs) >= 3:
                newfile = myargs[2]
                r = k.set_contents_from_filename(myargs[2])
            else:
                r = k.set_contents_from_file(sys.stdin)
                if verbose: print r
        except:
            print >>sys.stderr, sys.exc_info()
            if newfile is not None:
                print >>sys.stderr, "cleanup", newfile
                os.unlink(newfile)
            return 1
    
    return 0

class Chunk:
    """ A chunk is a view of a big file starting at offset and with a limited size"""
    
    def __init__(self, name, size, offset):
        self.name = name; self.size = size; self.offset = offset


def put_big_file(bucket, bucketname, big_filename, dname):
    statinfo = os.stat(big_filename)
    assert S_ISREG(statinfo[ST_MODE])

    os.mkdir(dname)

    # get an md5 sum of the big file
    md5 = get_md5sum(big_filename)

    # make chunks
    if split:
        chunks = make_chunks_using_split(big_filename, dname)
    else:
        chunks = make_chunks_using_fslice(big_filename, dname)
    
    # upload chunks to S3
    for chunk in chunks:
        if verbose:
            print "put %s %s" % (bucketname, chunk.name)
        if debug == 0:
            file = chunk.open()
            put_file(bucket, bucketname, chunk.name, file)
            file.close()

    # generate a big file descriptor
    xmlfilename = big_filename + ".xml"
    if verbose: print xmlfilename
    write_big_file_descriptor(dname + "/" + xmlfilename, big_filename, md5, chunks)

    # upload the descriptor to S3
    if verbose:
        print "put %s %s" % (bucketname, xmlfilename)
    if debug == 0:
        file = open(dname + "/" + xmlfilename, "rb")
        put_file(bucket, bucketname, xmlfilename, file)
        file.close()

    if debug:
        return

    # cleanup
    for chunk in chunks:
        chunk.unlink()
    os.unlink(dname + "/" + xmlfilename)
    os.rmdir(dname)

def get_md5sum(big_filename):
    md5filename = big_filename + ".md5"
    if os.path.isfile(md5filename):
        md5file = open(md5filename, "r")
    else:
        cmd = "md5sum " + big_filename
        if verbose: print cmd
        md5file = os.popen(cmd)
    md5 = md5file.read().split()
    if verbose: print md5
    assert len(md5) == 2 and md5[1] == big_filename
    md5 = md5[0]
    md5file.close()
    return md5

class FileChunk(Chunk):
    """ A file chunk is a view of a big file represented as a file in a temp directory"""
    
    def __init__(self, dname, name, size, offset):
        Chunk.__init__(self, name, size, offset)
        self.dname = dname;

    def open(self):
        return open(self.dname + "/" + self.name, "rb")

    def unlink(self):
        os.unlink(self.dname + "/" + self.name)

def make_chunks_using_split(big_filename, dname):
    chunksize = get_chunksize(chunksizestr)
    statinfo = os.stat(big_filename)
    if statinfo[ST_SIZE] > chunksize:
        # split the file into chunks suitable to S3
        cmd = "split -b %s %s %s/%s." % (chunksizestr, big_filename, dname, big_filename)
        if verbose: print cmd
        r = os.system(cmd)
        assert r == 0
    else:
        if verbose: print "link", big_filename
        os.link(big_filename, dname + "/" + big_filename)

    os.chdir(dname)

    files = glob.glob("*")
    files.sort()
    if verbose: print files

    chunks = []
    if len(files) == 1:
        chunks.append(FileChunk(dname, files[0], None, None))
    else:
        offset = 0
        for file in files:
            statinfo = os.stat(file)
            chunks.append(FileChunk(dname, file, statinfo[ST_SIZE], offset))
            offset += statinfo[ST_SIZE]

    os.chdir("..")

    return chunks

class Fslice:
    """ A file slice is a view of a big file that remaps file operationss"""
    
    def __init__(self, name, size, offset):
        self.name = name; self.size = size; self.base = offset; self.limit = self.base + self.size
        self.fp = None

    def __del__(self):
        self.fp.close()

    def open(self):
        self.fp = open(self.name, "rb")
        self.fp.seek(self.base)
        return self.fp

    def seek(self, offset):
        self.fp.seek(self.base + offset)

    def tell(self):
        offset = self.fp.tell()
        if offset < self.base or offset > self.limit:
            offset = None
        else:
            offset -= self.base
        return offset

    def read(self, N):
        offset = self.fp.tell()
        if offset < self.base or offset > self.limit:
            return None
        if offset + N > self.limit:
            N = self.limit - offset
        return self.fp.read(N)

    def close(self):
        self.fp.close()

class SlicedChunk(Chunk):
    """ A sliced chunk is a subset of a big file that uses a file slice to read it"""
    
    def __init__(self, filename, name, size, offset):
        Chunk.__init__(self, name, size, offset)
        self.fslice = Fslice(filename, self.size, self.offset)

    def open(self):
        self.fslice.open()
        return self.fslice

    def unlink(self):
        pass

def make_chunks_using_fslice(big_filename, dname):
    chunks = []
    chunksize = get_chunksize(chunksizestr)
    statinfo = os.stat(big_filename)
    if statinfo[ST_SIZE] > chunksize:
        offset = 0
        i = 0
        while offset < statinfo[ST_SIZE]:
            size = chunksize;
            if offset + size > statinfo[ST_SIZE]:
                size = statinfo[ST_SIZE] - offset
            chunks.append(SlicedChunk(big_filename, big_filename + "." + str(i), size, offset))
            offset += size
            i += 1
    else:
        if verbose: print "link", big_filename
        os.link(big_filename, dname + "/" + big_filename)
        chunks.append(FileChunk(dname, big_filename, None, None))
    return chunks

def put_file(bucket, bucketname, key, file):
    k = boto.s3.key.Key(bucket)
    k.key = key
    r = k.set_contents_from_file(file)
    if verbose: print r

def get_chunksize(chunksizestr):
    cs = chunksizestr
    m = cs[-1]
    if m == 'k':
        cs = cs[:len(cs)-1]
        cs = int(cs) * 1024
    elif m == 'm':
        cs = cs[:len(cs)-1]
        cs = int(cs) * 1024 * 1024
    else:
        cs = int(cs)
    return cs

import xml.dom.minidom

def createTagData(doc, tag, data):
    e = doc.createElement(tag)
    e.appendChild(doc.createTextNode(data))
    return e

def createChunk(doc, chunk):
    if chunk.size == None and chunk.offset == None:
        return createTagData(doc, "chunk", chunk.name)
    else:
        chunke = doc.createElement("chunk")
        chunke.setAttribute("name", chunk.name)
        if chunk.size != None:
            chunke.setAttribute("size", str(chunk.size))
        if chunk.offset != None:
            chunke.setAttribute("offset", str(chunk.offset))
    return chunke

def write_big_file_descriptor(xmlfilename, big_filename, md5, chunks):
    dom = xml.dom.minidom.getDOMImplementation()
    doc = dom.createDocument(None, None, None)
    bigfile = doc.createElement("bigfile")
    doc.appendChild(bigfile)
    bigfile.appendChild(createTagData(doc, "filename", big_filename))
    bigfile.appendChild(createTagData(doc, "md5", md5))
    for chunk in chunks:
        bigfile.appendChild(createChunk(doc, chunk))
    f = open(xmlfilename, "w")
    f.write(doc.toxml())
    f.close()

sys.exit(main())
