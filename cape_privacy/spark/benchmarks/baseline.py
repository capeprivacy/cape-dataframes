#!/usr/bin/python
import argparse
import time

import numpy as np
import pandas as pd
from pyspark import sql
from pyspark.sql import functions
from pyspark.sql import types


def make_args():
    parser = argparse.ArgumentParser(description='baseline benchmark')
    parser.add_argument('--runner', type=str)
    parser.add_argument('--disable-arrow', default=False, action='store_true')
    parser.add_argument('--csv-path', type=str, required=False, default='data/application_with_pii.csv')
    parser.add_argument('--runs', type=int, required=False, default=100)
    return parser


def make_spark_sess(use_arrow, name=None):
    sps = sql.SparkSession.builder
    if name is None:
        name = 'benchmarks.baseline'
    sps = sps.appName(name)
    if use_arrow:
        sps = sps.config('spark.sql.execution.arrow.enabled', 'true')
    return sps.getOrCreate()


def noop_runner(df):
    return df


def show_runner(df, n=5):
    df.show(n)
    return df


def user_task(df):
    return df.describe()

  
_RUNNERS = {"noop": noop_runner, "show": show_runner}

def main(args):
    use_arrow = not args.disable_arrow
    sess = make_spark_sess(use_arrow)
    df = sess.read.load(args.csv_path,
        format='csv', header='true', infer_schema='true', sep=',')
    # which "transformation" to run
    runner = _RUNNERS.get(args.runner, None)
    if runner is None:
        raise NotImplementedError
    # measure & store run time
    deltas = []
    for _ in range(args.runs):
        t0 = time.time()
        result = runner(df)
        done = user_task(result)
        t1 = time.time()
        deltas.append(t1 - t0)
    print('Duration mean (variance): {:f} ({:f})'.format(np.mean(deltas), np.var(deltas)))

if __name__ == '__main__':
    parser = make_args()
    args = parser.parse_args()
    main(args)
