# data
Just a local directory for storing datasets, in case you don't want to mount the GCS bucket locally.
You can populate this with data from a GCS bucket by running
```
gsutil cp -r gs://<bucket-name>/credit/ ./data
```
from root.