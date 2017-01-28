# The AWS S3 tools
The AWS S3 tools are modeled on the unix directory and file abstraction.
An S3 bucket is like a unix directory, and an S3 key is like a unix file.
One can create and remove buckets, put files into buckets, get files from buckets, and list
the contents of buckets.   There are some simple but incomplete tools to 
control bucket and key permissions.

## Boto
The boto and boto3 libraries are the python interfaces to the Amazon Web Services.  
They can be found at https://github.com/boto.

### Installing boto
See the boto and boto3 README's.

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

Bundled files are not supported since the S3 object size limit is now 5 TB.

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
    $ s3ls -l BUCKET | gawk -M '{s+=$3}END{print s};
    $ sels -l --prefix=PREFIX BUCKET | gawk -M '{s+=$3}END{print s}'

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

