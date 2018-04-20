#!/bin/sh
set -e

# copy all of the objects from a source bucket to a destination bucket.
# implementation: use aws cli utility.
# problem:  metadata (including user_md5 checksum) not copied during multipart copy.
# solution: set the multipart threshold bigger than any object being copied
# alternate solution: fixup the metadata for all copied objects with missing metadata.
# (not implemented yet).

aws configure set default.s3.multipart_threshold 1TB
aws s3 cp --recursive $*

