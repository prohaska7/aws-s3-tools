#!/usr/bin/env bash
set -e
verbose=0
while [ $# -gt 0 ] ; do
    if [ "$1" = "--verbose" ] ; then verbose=1; shift; else break; fi
done
if [ $# != 2 ] ; then exit 1; fi
bucket=$1
find -L $2 -type f | \
while read f; do
    if [ -f "$f" ] ; then
        if [ $verbose -ne 0 ] ; then echop s3put $bucket "$f" "$f"
	s3put $bucket "$f" "$f"
    fi
done
