# The AWS S3 tools
The AWS S3 tools are modeled on the unix directory and file abstraction.
An S3 bucket is like a unix directory, and an S3 key is like a unix file.
One can create and remove buckets, put files into buckets, get files from buckets, and list
the contents of buckets.   There are some simple but incomplete tools to 
control bucket and key permissions.

S3 objects have limitations on size.  The last time I checked, the limitation was about 5TiB.
These tools use bundled files, described later, to store large files.

These tools use the boto python library, which must be downloaded and installed.

## Boto
The boto library is the python interface to the Amazon Web Services.  
It can be found at http://code.google.com.

### Installing boto
    $ download and untar the boto tarball
    $ cd boto-WHATEVER
    $ python setup.py build
    $ sudo python setup.py install

## Installing the AWS tools
The makefile installs the tools in the user's bin directory.

    $ make install

## Setting up AWS access keys
    export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY

## Bundled files
Since S3 objects have limitations on sizes, these tools allow one to store a file in a sequence of S3 objects
called bundled files.  The bundled file is described by an XML coded meta-data file, which contains an MD5 sum
of the file.  It also contains the name, offset, and size of all of the S3 objects that the original file is 
decomposed into.  

## API

### List the S3 buckets
    $ s3ls

### Make an S3 bucket
    $ s3mkbucket NEWBUCKET

### List the keys in an S3 bucket
    $ s3ls -l BUCKET

## List the keys with a given PREFIX
   $ s3ls --prefix=PREFIX BUCKET

### List the newest keys
    $ s3ls -l BUCKET | sort | tail

### Compute the sum of the key sizes in a bucket
    $ s3ls --select=size BUCKET | awk '{s+=$1}END{print s};
    $ sels --select=size --prefix=PREFIX BUCKET | awk '{s+=$1}END{print s}'

### Put a file into an S3 bucket
    $ s3put BUCKET KEY FILE

### Put a bundled  file into an S3 bucket
    $ s3put --bundle BUCKET FILE

### Get an object from an S3 bucket
    $ s3get BUCKET KEY OUTFILE

### Get a bundled file from an S3 bucket
    $ s3get --bundle BUCKET FILE

### Delete a key from an S3 bucket
    $ s3rm BUCKET KEY

