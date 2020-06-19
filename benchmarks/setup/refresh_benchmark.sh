#!/bin/sh

SCRIPT=$1
BUCKET=$2
if [ $# -ne 2 ]
  then
    echo "ERROR: Wrong number of arguments supplied. Requires 'script' and 'bucket' arguments."
    exit 1
fi

gsutil cp ${SCRIPT} gs://${BUCKET}