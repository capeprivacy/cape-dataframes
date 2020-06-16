# benchmarks


# Running on GCP
Beware a bit of hackery; we can make this cleaner in the future :)

If you haven't already, you need to set the dataproc region:

```sh
gcloud config set dataproc/region us-central1
```

Say you have a local PySpark script, in this example `baseline.py`, that you want to submit to a Dataproc cluster for benchmarking.
First, copy the script to the staging bucket with 

```sh
BUCKET=dataproc-staging-us-central1-1082497657493-tqtpspxa
CLUSTER=arrow-dev-test-03
PROFILER=profiler-01
```


```sh
SCRIPT=baseline.py

gsutil cp ${SCRIPT} gs://${BUCKET}
```

The command `./setup/refresh_benchmark.sh ${SCRIPT} ${BUCKET}` is an alias.

You will also have to run the following to allow the `cape_spark` repo to be reachable from the cluster workers:
```sh
pip install -Ut cape_dependency .
zip -r cape_dependency.zip cape_dependency
gsutil cp cape_dependency.zip gs://${BUCKET}
```

You can also do this automatically by running the following from root:
```sh
./setup/refresh_dependency.sh ${BUCKET}
```

Now the script is ready to be used in a Dataproc job:

```sh
gcloud dataproc jobs submit pyspark gs://${BUCKET}/${SCRIPT} \
  --cluster=${CLUSTER} \
  -- \
  --runner round \
  --csv-path gs://${BUCKET}/credit/application_with_pii.csv \
  --runs 5 \
  --dependency-path ${BUCKET}/cape_dependency.zip
```

With profiler:

```sh
gcloud dataproc jobs submit pyspark gs://${BUCKET}/${SCRIPT} \
  --cluster=${CLUSTER} \
  --properties=cloud.profiler.enable=true,cloud.profiler.name=${PROFILER} \
  -- \
  --runner round \
  --csv-path gs://${BUCKET}/credit/application_with_pii.csv \
  --runs 5 \
  --dependency-path ${BUCKET}/cape_dependency.zip
```
