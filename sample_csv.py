#!/usr/bin/env python3
import argparse
import glob
import os
import random
import sys

import pandas as pd
import pyarrow.parquet as pt
from pyarrow.parquet import ParquetFile

global_sample_rate = 100
parquet_to_pd_type_map = {
    'float': 'float32',
    'string': 'str',
    'int64': 'int64',
    'double': 'float64',
}


def get_parquet_data_types(parquet_file_name):
    # pf = ParquetFile(parquet_file_name)
    # md = pt.read_metadata(parquet_file_name)
    schema = pt.read_schema(parquet_file_name)
    pd_types = map(parquet_to_pd_type_map, schema.types)
    return pd_types


def sample_csv(f, sample_rate, data_types):
    # reference from https://nikgrozev.com/2015/06/16/fast-and-simple-sampling-in-pandas-when-loading-data-from-files/
    num_lines = sum(1 for l in open(f))
    size = int(num_lines / sample_rate)

    # The row indices to skip - make sure 0 is not included to keep the header!
    try:
        skip_idx = random.sample(range(1, num_lines), num_lines - size)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

    # Read the data
    # df = pd.read_csv(f, skiprows=skip_idx, header=False)
    # df = pd.read_csv(f, skiprows=skip_idx, low_memory=False, dtype=data_types)
    df = pd.read_csv(f, skiprows=skip_idx, low_memory=False)
    return df


def read_write_sampled_csv(f, sample_rate=100):
    print("file to sample: ", f)
    dir_name = os.path.dirname(f)
    parquet_file = glob.glob1(dir_name, "*.parquet")
    if len(parquet_file) > 0:
        parquet_file = parquet_file[0]
    else:
        return

    data_types = get_parquet_data_types(dir_name + "/" + parquet_file)
    sampled_df = sample_csv(f, sample_rate, data_types)
    root_name, ext = os.path.splitext(f)
    out_file_name = root_name + "_sampled_" + str(sample_rate) + ext
    sampled_df.to_csv(out_file_name, index=False, header=False)
    return out_file_name


def try_sample_file(f):
    ext = os.path.splitext(f)[1]
    if ext != '.csv':
        return
    sample_file_name = read_write_sampled_csv(f, global_sample_rate)
    if sample_file_name is not None and os.path.isfile(sample_file_name):
        return sample_file_name


def process_path(path, f, files):
    # for f in glob.glob1(path):
    f = path + "/" + f
    if os.path.isdir(f):
        new_files = traverse_sample(f)
        files.append(new_files)
    else:
        res = try_sample_file(f)
        if res is not None:
            files.append(res)
    return files


def traverse_sample(path):
    files = []
    if os.path.isfile(path):
        res = try_sample_file(path)
        if res is not None:
            files.append(res)
        return files

    # parallelize the loop... (since this can take a while
    # TODO(): make the intensive loop parallel
    # pool = multiprocessing.Pool(4)
    # files = zip(*pool.map(process_path, os.listdir(path))
    for f in os.listdir(path):
        process_path(path, f, files)
    return files


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='Print log messages')
    parser.add_argument('-s', '--samplerate', action='store', type=int, metavar='RATE', help='sampling rate for files')
    parser.add_argument('-p', '--path', action='store', type=str, metavar='DIR', nargs='+',
                        help='input directory list or file list of csv files')

    return parser


def test():
    mode = 'combine'
    test_path = './data/example'
    # traverse_sample(test_path)


def main():
    parser = parse_arguments()
    # args = parser.parse_known_args()
    args = parser.parse_args()

    files = args.path

    if args.samplerate:
        global global_sample_rate
        global_sample_rate = args.samplerate
        print("sample rate is set to: ", global_sample_rate)

    print("sample_csv arguments: " + str(files))
    for f in files:
        # read_write_sampled_csv(f)
        traverse_sample(f)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main()
    else:
        print("cmd line argrument error!!")
        exit(1)
