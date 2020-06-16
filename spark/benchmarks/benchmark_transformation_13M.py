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


parser = argparse.ArgumentParser(description='baseline benchmark')
parser.add_argument('--runner', type=str)
parser.add_argument('--run-all', default=False, action='store_true')
parser.add_argument('--user-task', type=str, default='collect')
parser.add_argument('--disable-arrow', action='store_true', default=False)
parser.add_argument('--local', action='store_true', default=False)
parser.add_argument('--dependency-path', type=str, default='cape_dependency.zip')
parser.add_argument('--csv-path', type=str, required=False, default='data/application_with_pii.csv')
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

def one_string_runner(df):
    return df.select(df.name)

def one_numeric_runner(df):
    return df.select(df.AMT_PAYMENT)


def all_col_runner(df):
    return df.select('*')


def tokenize_1c_runner(df):
    out = df.select(tokenizer_udf(df.name))
    return out


def tokenize_4c_runner(df):
    out = df.select([tokenizer_udf(df.name), tokenizer_udf(df.city)])
    return out

def mask_entire_dataset(df):
    out = df.select([tokenizer_udf(df.name), 
                    tokenizer_udf(df.city),
                    tokenizer_udf(df.birthdate),
                    tokenizer_udf(df.street_address),
                    tokenizer_udf(df.zipcode),
                    tokenizer_udf(df.SK_ID_CURR),
                    tokenizer_udf(df.SK_ID_PREV),
                    native_rounder(df.AMT_PAYMENT),
                    native_rounder(df.AMT_INSTALMENT),
                    native_rounder(df.DAYS_ENTRY_PAYMENT),
                    native_rounder(df.NUM_INSTALMENT_VERSION),
                    perturb_udf(df.NUM_INSTALMENT_NUMBER)
                    ])
    return out


def tokenize_1c_return_all_col_runner(df):
    colnames = df.schema.names
    col_to_token = ['name']
    out = df.select([tokenizer_udf(functions.col(c)) if c in col_to_token 
        else functions.col(c) for c in colnames])
    return out


def perturbation_runner(df):
    out = df.select(perturb_udf(df.AMT_PAYMENT))
    return out


def rounding_runner(df):
    out = df.select(round_fn_udf(df.AMT_PAYMENT))
    return out


def native_rounding_runner(df):
    out = df.select(native_rounder(df.AMT_PAYMENT))
    return out

_RUNNERS = {
    'one-string': one_string_runner,
    'one-numeric': one_numeric_runner,
    'all-col': all_col_runner,
    'tokenize-1c': tokenize_1c_runner,
    'tokenize-4c': tokenize_4c_runner,
    'tokenize-1c-all': tokenize_1c_return_all_col_runner,
    'mask-entire-dataset': mask_entire_dataset,
    'perturb': perturbation_runner,
    'round': rounding_runner,
    'round-native': native_rounding_runner,
}
_TASKS = {
    'collect': collect_task,
    'describe': describe_task,
}


def main():
    df = sess.read.load(args.csv_path,
        format='csv', header='true', infer_schema='true', sep=',')
    # NOTE with the import above all the columns end up being a string type
    # add to cast for benchmarking
    string_to_num = ['NUM_INSTALMENT_VERSION', 
                     'NUM_INSTALMENT_NUMBER', 'DAYS_INSTALMENT', 'DAYS_ENTRY_PAYMENT',
                     'AMT_INSTALMENT','AMT_PAYMENT']
    for c in string_to_num:
        df = df.withColumn(c, df[c].cast(types.FloatType()))
    # which transformation (runner) to run
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
        time_operation(df, runner, collect_task, name)


if __name__ == '__main__':
    main()
