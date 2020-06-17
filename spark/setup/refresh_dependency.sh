#!/bin/sh

BUCKET=$1
if [ -z "$1" ]
  then
    echo "ERROR: No 'bucket' argument supplied"
    exit 1
fi

pip install -Ut cape_dependency .
cd cape_dependency
zip -r ../cape_dependency.zip .
gsutil cp ../cape_dependency.zip gs://${BUCKET}