# Our AWS S3 tools
Our AWS S3 tools are modeled on the unix directory and file abstraction.
An S3 bucket is like a unix directory, and an S3 key is like a unix file.
One can create and remove buckets, put files into buckets, get files from buckets, and list
the contents of buckets.   There are some simple but incomplete tools to 
control bucket and key permissions.

S3 keys have limitations on size.  The last time i checked, the limitation was about 2GiB.
These tools implemented bundled files, which are files that are split into several smaller
S3 objects and described by an XML meta-data file.

The tools use the boto python library, which must be downloaded and installed.

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

## API

### List the S3 buckets
    $ s3ls

### Make an S3 bucket
    $ s3mkbucket NEWBUCKETNAME

### List the keys in an S3 bucket
    $ s3ls -l BUCKETNAME

### Compute the sum of the key sizes in a bucket
    $ s3ls --select=size tokutek-mysql-build | awk '{s+=$1}END{print s};

### Put a file into an S3 bucket
    $ s3put BUCKETNAME KEY FILENAME

### Put a bundled  file into an S3 bucket
    $ s3put --bundle BUCKETNAME FILENAME

### Get an object from an S3 bucket
    $ s3get BUCKETNAME KEY outfile

### Get a bundled file from an S3 bucket
    $ s3get --bundle BUCKETNAME FILENAME

### Delete a key from an S3 bucket
    $ s3rm BUCKETNAME KEY

