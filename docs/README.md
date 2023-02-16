# Cape Dataframes overview

Cape Dataframes allows you to write data privacy policies and data transformations to integrate with [Pandas](https://pandas.pydata.org/) and [Spark](https://spark.apache.org/).

You can view the source code in the [Cape Dataframes GitHub Repository](https://github.com/capeprivacy/cape-dataframes).

## Use cases

Review the [transformations](transformations.md) and decide which are a good fit for your data science needs. 

The 0.1.0 release includes five transformations that provide some common privacy protections. 

| Use case  | Text data | Numeric data | Inconsistent data
| ------------- | ------------- | --------------- | -----------
| EDA | Tokenization  | Rounding or pertubation | Tokenization 
| Analytics | Tokenization  | Rounding or pertubation | -
| ML development | - | Rounding or pertubation | Tokenization
| ML training/serving | No transformation | No transformation | No transformation

Cape Dataframes will support more use cases through additional transformations in future releases.
