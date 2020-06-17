#!/usr/bin/python
import argparse
import uuid
import hashlib
import time

import numpy as np
import pandas as pd
import pyspark
from pyspark import sql
from pyspark.sql import functions
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType, DoubleType


parser = argparse.ArgumentParser(description='baseline benchmark')
parser.add_argument('--runner', type=str)
parser.add_argument('--run-all', default=False, action='store_true')
parser.add_argument('--user-task', type=str, default='collect')
parser.add_argument('--disable-arrow', action='store_true', default=False)
parser.add_argument('--local', action='store_true', default=False)
parser.add_argument('--dependency-path', type=str, default='cape_dependency.zip')
parser.add_argument('--dataset-size', type=str, required=False, default='small')
parser.add_argument('--runs', type=int, required=False, default=10)
args = parser.parse_args()

sess_builder = sql.SparkSession.builder
sess_builder = sess_builder.appName('benchmarks')
if not args.disable_arrow:
    sess_builder = sess_builder.config('spark.sql.execution.arrow.enabled', 'true')
sess = sess_builder.getOrCreate()
if not args.local:
    sess.sparkContext.addPyFile(args.dependency_path)

from cape_spark import transformations as tfm
from cape_spark import types

def time_operation(df, runner, user_task, name):
    # measure & store run time
    deltas = []
    for _ in range(args.runs):
        t0 = time.time()
        result = runner(df)
        done = user_task(result)
        t1 = time.time()
        deltas.append(t1 - t0)
    print('{} - Duration mean in seconds (variance): {:f} ({:f})'.format(
        name, np.mean(deltas), np.var(deltas)))

def collect_task(df):
    return df.collect()

def describe_task(df):
    # mostly useful for local development
    return df.describe()


# Tranformation fn
perturb = tfm.Perturbation(types.Float,
    low_boundary=-100, high_boundary=100)
perturb_udf = functions.pandas_udf(perturb, returnType=types.Float)

rounder = tfm.Rounding(types.Float, number_digits=1)
round_fn_udf = functions.pandas_udf(rounder, returnType=types.Float)

native_rounder = tfm.NativeRounding(types.Float, number_digits=1)

tokenizer_tfm = tfm.Tokenizer(key='123')
tokenizer_series = lambda series: series.map(tokenizer_tfm)

tokenizer_udf = functions.pandas_udf(tokenizer_series, returnType=types.String)


# Runners for benchmarking

def one_col_runner(df):
    return df.select(df.store_and_fwd_flag)

def one_numeric_runner(df):
    return df.select(df.total_amount)

def all_col_runner(df):
    return df.select('*')


def tokenize_1c_runner(df):
    out = df.select(tokenizer_udf(df.store_and_fwd_flag))
    return out



def perturbation_runner(df):
    out = df.select(perturb_udf(df.total_amount))
    return out


def rounding_runner(df):
    out = df.select(round_fn_udf(df.total_amount))
    return out


def native_rounding_runner(df):
    out = df.select(native_rounder(df.total_amount))
    return out

def mask_entire_dataset(df):
    out = df.select([tokenizer_udf(df.vendor_id), 
                    tokenizer_udf(df.store_and_fwd_flag),
                    tokenizer_udf(df.payment_type),
                    native_rounder(df.passenger_count),
                    native_rounder(df.trip_distance),
                    native_rounder(df.pickup_longitude),
                    native_rounder(df.pickup_latitude),
                    native_rounder(df.payment_type),
                    native_rounder(df.fare_amount),
                    native_rounder(df.extra),
                    perturb_udf(df.mta_tax),
                    perturb_udf(df.tip_amount),
                    perturb_udf(df.tolls_amount),
                    perturb_udf(df.imp_surcharge),
                    perturb_udf(df.total_amount),
                    ])
    return out

_RUNNERS = {
    'one-col': one_col_runner,
    'one-numeric': one_numeric_runner,
    'all-col': all_col_runner,
    'tokenize-1c': tokenize_1c_runner,
    'perturb': perturbation_runner,
    'round': rounding_runner,
    'round-native': native_rounding_runner,
    'mask-entire-dataset': mask_entire_dataset,
}
_TASKS = {
    'collect': collect_task,
    'describe': describe_task,
}

small = ['tlc_yellow_trips_2009']
medium = small + ['tlc_yellow_trips_2010', 'tlc_yellow_trips_2011']
large = medium + ['tlc_yellow_trips_2012', 'tlc_yellow_trips_2013',
                  'tlc_yellow_trips_2014', 'tlc_yellow_trips_2015', 
                  'tlc_yellow_trips_2016']

_DATASET_SIZE = {
    'small': small,
    'medium': medium,
    'large': large,
}


def main():

    fields = [StructField("vendor_id", StringType(), False),
              StructField("pickup_datetime", TimestampType(), True), 
              StructField("dropoff_datetime", TimestampType(), True), 
              StructField("passenger_count", LongType(), True), 
              StructField("trip_distance", DoubleType(), True),
              StructField("pickup_longitude", DoubleType(), True),
              StructField("pickup_latitude", DoubleType(), True),
              StructField("rate_code", LongType(), True), 
              StructField("store_and_fwd_flag", StringType(), True), 
              StructField("dropoff_longitude", DoubleType(), True), 
              StructField("dropoff_latitude", DoubleType(), True), 
              StructField("payment_type", StringType(), True), 
              StructField("fare_amount", DoubleType(), True), 
              StructField("extra", DoubleType(), True), 
              StructField("mta_tax", DoubleType(), True), 
              StructField("tip_amount", DoubleType(), True), 
              StructField("tolls_amount", DoubleType(), True), 
              StructField("imp_surcharge", DoubleType(), True), 
              StructField("total_amount", DoubleType(), True)]
    
    schema = StructType(fields)
    df_combined = sess.createDataFrame([], schema)

    table_list = _DATASET_SIZE.get(args.dataset_size)
    # Keep track of all tables accessed via the job
    tables_read = []
    for table in table_list:
        table_path = f"bigquery-public-data.new_york.{table}"
        df = (sess.read.format('bigquery').option('table', table_path).load())
        tables_read.append(table_path)

        df_combined = (
            df
            .union(df_combined)
        )

    for table in tables_read:
        print(table)

    print("TAXI SIZE: ", df_combined.count())
    which transformation (runner) to run
    assert args.run_all or args.runner is not None
    if args.run_all:
        runners = _RUNNERS
    else:
        runner = _RUNNERS.get(args.runner, None)
        if runner is None:
            raise NotImplementedError
        runners = {args.runner: runner}
    # run the transformations
    for name, runner in runners.items():
        time_operation(df, runner, describe_task, name)


if __name__ == '__main__':
    main()
