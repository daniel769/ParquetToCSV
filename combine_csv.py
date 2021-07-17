#!/usr/bin/env python3
import os
import sys

from pathlib import Path

sys.path.append(os.getcwd())


def merge_files(file_list, merged_file_name):
    try:
        with open(merged_file_name, 'w') as outfile:
            for file_name in file_list:
                print("merging " + str(file_name))
                with open(file_name) as infile:
                    outfile.write(infile.read())
    except Exception as e:
        print(e)


# either filelist or dataframe list
def combine(dir_name, file_list):
    ext_csv = 'csv'
    # file_list = [i for i in glob.glob('*_sampled*.{}'.format(extension))]
    # file_list = file_list
    if file_list is None:
        file_list = [i for i in Path(dir_name).rglob('*_sampled_*.{}'.format(ext_csv))]

    merge_file_name = dir_name + "/combined_csv.csv"
    merge_files(file_list, merge_file_name)


def main():
    files = sys.argv[1:]
    for f in files:
        # read_write_sampled_csv(f)
        combine(f, None)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main()
    else:
        path = './data/example'
        extension = 'csv'
        sampled_files = [i for i in Path(path).rglob('*_sampled_*.{}'.format(extension))]
        combine(path, sampled_files)

# left overs:
'''
    df_list = []
    for f in file_list:
        df = pd.read_csv(f, header=False, ignore_index=False)
        df_list.append[df]

    result = df_list[0].append(df_list[1:], ignore_index=True, sort=False)
    result.to_csv(out_file_name, index=False, header=False)
    return result
'''

'''
def sample_and_combine_csv(sample_path):
    """
    files = []
    for f in os.listdir(path):
        if os.path.isdir(f):
            new_files = sample_and_combine_csv(f)
            files.append(new_files)
        else:
            sample_fname = read_write_sampled_csv(f)
            if os.path.isfile(sample_fname):
                files.append(sample_fname)
                return files
    """
    sampled_files = traverse_sample(sample_path)
    # sampled_files = [i for i in glob.glob('*_sampled*.{}'.format('csv'))]
    combined_file_name = combine(sample_path, sampled_files)
    return combined_file_name
'''
