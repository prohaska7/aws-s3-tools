#!/usr/bin/env bash
set -e
if [ $# != 2 ] ; then exit 1; fi
bucket=$1
s3ls $bucket --prefix="$2" | \
while read file; do
    d=$(dirname "$file")
    f=$(basename "$file")
    if [ ! -d "$d" ] ; then mkdir -p "$d"; fi
    if [ ! -f "$d"/"$f" ] ; then 
        pushd "$d"
        s3get $bucket "$file" "$f"
        popd
    fi
done
