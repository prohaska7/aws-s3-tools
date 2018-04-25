#!/usr/bin/env bash
set -e
verbose=0
while [ $# -gt 0 ] ; do
    if [ "$1" = "--verbose" ] ; then verbose=1; shift; else break; fi
done
if [ $# != 2 ] ; then exit 1; fi
bucket=$1
s3ls $bucket --prefix="$2" | \
while read file; do
    d=$(dirname "$file")
    f=$(basename "$file")
    if [ ! -d "$d" ] ; then mkdir -p "$d"; fi
    if [ ! -f "$d"/"$f" ] ; then 
        pushd "$d" >/dev/null
        if [ $verbose -ne 0 ] ; then echo s3get $bucket "$file" "$f"; fi
        s3get $bucket "$file" "$f"
        popd >/dev/null
    fi
done
