'''Summarize the data.'''

import ast
import re

zfs_line_pattern = r'^# {.*}$'
zfs_line_pattern = re.compile(zfs_line_pattern)



# TODO store the zfs params in a better way to avoid ad-hoc parsing
def parse_zfs_params(path):
    '''Given the path to a file,
    try to find and parse the zfs params.
    '''

    # get the line of the file
    with open(path, 'r') as f:
        lines = f.read().split('\n')

    # look for a line that is of the form # {*}
    for line in lines:
        m = zfs_line_pattern.match(line)
        if m:
            zfs_line = m.group()

    # now grab just the part that is a dict
    dict_part = zfs_line[zfs_line.index('{'):]

    # now interpret it into a dict
    return ast.literal_eval(dict_part)

def get_data_from_mdtest(mdtest_logs_path):
    '''Parse the output of mdtest and get the data
    you crave!
    '''

    def grab_tokens(line):
        tokens = line.split()
        names = ('max', 'min', 'mean', 'stddev')
        return {n: t for n,t in zip(names, tokens[3:])}



    with open(mdtest_logs_path, 'r') as f:
        lines = f.read().splitlines()

    # find where the actual summary data starts
    # There are two sections "SUMMARY rate" and "SUMMARY time"
    state = 'neither'
    stats = {'rate': {}, 'time': {}}
    summary_vals = [
        'File creation',
        'File stat',
        'File read',
        'File removal',
        'Tree creation',
        'Tree removal',
    ]
    for line in lines:
        if 'SUMMARY rate' in line:
            state = 'rate'
        elif 'SUMMARY time' in line:
            state = 'time'

        for val in summary_vals:
            if val in line and state in ('rate', 'time'):
                val_snake = val.lower().replace(' ', '_')
                stats[state][val_snake] = grab_tokens(line)

    return stats
