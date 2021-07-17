#!/usr/bin/env python3
import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa


def target_csv_name(filename):
    csv_filename = []
    try:
        file_wo_ext = os.path.splitext(filename)[0]
        csv_filename = file_wo_ext + ".csv"
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

    return csv_filename


def get_timestamp(filename):
    import re
    # add timestamp column if the filename contains a timestamp
    has_timestamp_regex = re.compile("=(\\d+)\\/.+")
    res = has_timestamp_regex.search(filename)
    if res is not None:
        sliced = res.group()
        timestamp = re.split("/", sliced)[0]
        timestamp = timestamp[1:-1]
        return timestamp


def convert_file(filename, csv_file_name, write_csv_with_header):
    # references: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    # use_pandas_metadata (boolean, default False)
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_pandas.html
    try:
        df = pd.read_parquet(filename)
        data_types = df.dtypes
        boolean_cols = [col for col in df.columns if df[col].dtypes == 'bool']
        df[boolean_cols] = df[boolean_cols] * 1

        timestamp = get_timestamp(filename)
        if timestamp is not None:
            df.insert(loc=0, column='timestamp', value=timestamp)

        # convert bool to numeric
        print(data_types)
        if os.path.exists(csv_file_name):
            # os.remove(csv_fname)
            return  # if csv exists, we won't do the work again
        # TODO(): check needs header or not
        # df.to_csv(csv_fname, index=False, header=False)
        df.to_csv(csv_file_name, index=False, header=write_csv_with_header)
        print("converted " + filename + "to " + csv_file_name)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)


# this is for parallel processing
# TODO(): make the program run in parallel on the file list
'''
def process_path(dirname, filename, csv_filenames, write_csv_with_header):
    parquet_ext = ".parquet"
    pathname = os.path.join(dirname, filename)
    if os.path.isfile(pathname):
        if filename.endswith(parquet_ext):
            csv_fname = target_csv_name(pathname);
            convert_file(pathname, csv_fname, write_csv_with_header)
            csv_filenames.append(csv_fname)
    else:
        convert_dir(pathname)
    # combine all files in the list
    return csv_filenames
'''


def convert_dir(dirname):
    parquet_ext = ".parquet"
    csv_file_names = []
    parquet_files = [f for f in os.listdir(dirname) if f.endswith(parquet_ext)]
    # parquet_count = len(glob.glob1(dir_name, parquet_ext))

    # this is for parallel processing
    # TODO(): make the program run in parallel on the file list
    '''
    pool = multiprocessing.Pool(4)
    files = zip(*pool.map(process_path, os.listdir(path))
    '''

    write_csv_with_header = len(parquet_files) > 1
    for filename in os.listdir(dirname):
        pathname = os.path.join(dirname, filename)
        if os.path.isfile(pathname):
            if filename.endswith(parquet_ext):
                csv_file_name = target_csv_name(pathname);
                convert_file(pathname, csv_file_name, write_csv_with_header)
                csv_file_names.append(csv_file_name)
        else:
            convert_dir(pathname)
        # combine all files in the list

    combined_name = os.path.join(dirname, "combined_csv.csv")
    if len(csv_file_names) > 1:
        combined_csv = pd.concat([pd.read_csv(f) for f in csv_file_names])
        # export to csv
        # combined_csv.to_csv( combined_name, index=False, encoding='utf-8-sig')
        combined_csv.to_csv(combined_name, index=False, header=False)
    elif len(csv_file_names) == 1:
        '''
        with open("test.csv",'r') as f:
            with open(combined_name,'w') as f1:
                next(f) # skip header line
                for line in f:
                    f1.write(line)
        '''
        os.rename(csv_file_names[0], combined_name)


# TODO(): change the program to accept named cmd line arguments
#  file_names = ParseArguments(sys.argv[1:])
def main():
    file_names = sys.argv[1:]
    for filename in file_names:
        if os.path.isfile(filename):
            convert_file(filename)
        else:
            convert_dir(filename)


def test():
    import pyarrow.parquet as pq
    df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                       'two': ['foo', 'bar', 'baz'],
                       'three': [True, False, True]},
                      index=list('abc'))

    table = pa.Table.from_pandas(df)
    filename = '/tmp/example.parquet'
    pq.write_table(table, filename)
    convert_file(filename)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main()
    else:
        # convert_dir('./data')
        convert_dir('./data/example')

# This is a better way to traverse files.
# TODO(): replace the traverse with this idiom in the future
'''
from os import walk

def list_files2(directory, extension):
    for (dirpath, dirnames, filenames) in walk(directory):
        return (f for f in filenames if f.endswith('.' + extension))
'''
