#!/usr/bin/env bash
set -e
if [ $# != 2 ] ; then exit 1; fi
bucket=$1
find $2 -type f | \
while read f; do
    if [ -f "$f" ] ; then
	s3put $bucket "$f" "$f"
    fi
done
