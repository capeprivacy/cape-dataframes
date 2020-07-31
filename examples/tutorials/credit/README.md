# Credit Risk Tutorial

This tutorial was created for the blog post [Cape Python: Apply Privacy-Enhancing Techniques to Protect Sensitive Data in Pandas and Spark](https://medium.com/dropoutlabs/cape-python-apply-privacy-enhancing-techniques-to-protect-sensitive-data-in-pandas-and-spark-e0bf8c0d55db).

As an example, we experiment with the public [German credit card dataset](https://archive.ics.uci.edu/ml/datasets/statlog+(german+credit+data)). We just added some fake PII information (such as name, address, etc.) and quasi-identifiers (city, salary etc.) to make it more similar to a real dataset for which we would use the masking techniques. 

## Prototype your privacy-preserving pipeline in Pandas

The notebook `mask_credit_data_in_pandas.ipynb` shows you how you can prototype the masking techniques in Pandas, and then define a data privacy policy.

## Make your Spark pipeline privacy-preserving
Once you have defined the data privacy policy, you can can apply it to a Spark DataFrame. As an example, you can run the following script:
```
# submit the script to a Spark cluster
spark-submit apply_policy_spark.py
```
