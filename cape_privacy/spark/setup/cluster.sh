#!/bin/sh

gcloud dataproc clusters create arrow-dev-test-02 \
    --bucket dataproc-staging-us-central1-1082497657493-tqtpspxa \
    --region us-central1 \
    --subnet default \
    --zone us-central1-b \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 1.5-debian10 \
    --project internal-260118 \
    --initialization-actions 'gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh' \
    --scopes monitoring \
    --metadata PIP_PACKAGES=pandas~=1.0.0
  
