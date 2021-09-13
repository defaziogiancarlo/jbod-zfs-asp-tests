'''Summarize the data.
Gather the outputs of dataruns, then put them
into a big table, which can them be printed, or used
in calculations.

The dataruns should all create a metadata file that has all the important
info needed to get the actual results.

The process is:

1.
Go to the meta-data directory
and read in all the metadata files
filter out only the metadatas you want based on various parameters
such as zfs, jbod, and date performed

2.
once you have the meta-data for all the runs you car about,
the meta-data files tell you where to go for the summary data,
which will be either a mdtest or ior output file.
The path and test_type are in the metadata, so the output will
be read and the metadata dict in memory will get updated with a
'results' section with all the results.

3.
The important data for the summary is then stored as results
in a way that can be printed out for csv, written out as yaml,
and used to do performance comparisons between jbod/zfs combos.
'''

# TODO maybe put it all into a pandas dataframe?
# you know you want to


import glob
import operator
import pathlib
import pprint
import re

#import pandas
import yaml

default_zfs_params = {
    'zfs_dirty_data_max': 4294967296,
    'zfs_dirty_data_max_percent': 10,
    'zfs_max_recordsize': 1048576
}

brian_zfs_params = {
    'zfs_dirty_data_max': 68719476736,
    'zfs_dirty_data_max_percent': 30,
    'zfs_max_recordsize': 16777216
}

brian_zfs_params_10 = {
    'zfs_dirty_data_max': 68719476736,
    'zfs_dirty_data_max_percent': 10,
    'zfs_max_recordsize': 16777216
}

## paths of great importance

ior_logs_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/ior_logs'
)
mdtest_logs_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/mdtest_logs'
)
srun_commands_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/srun_commands'
)

meta_data_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/test_meta_data'
)


## get the metadata, and filter for the stuff you care about

def all_meta_data():
    '''Get all the meta-data, but not the temp meta-data.'''

    paths = meta_data_dir.glob('*')
    meta_data = []
    for path in paths:
        with open(path, 'r') as f:
            meta_data.append(yaml.safe_load(f))
    return meta_data


## general srun output preprocessing

srun_proc_prefix = re.compile(r'^\s*\d+: ')

def remove_proc_number(line):
    return srun_proc_prefix.sub('', line, count=1)

def lines_without_proc_nums(path):
    with open(path, 'r') as f:
        return [remove_proc_number(line) for line in f]


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

def get_data_from_ior(ior_logs_path):
    '''Parse the output of ior and get the data
    you crave!
    '''

    lines = lines_without_proc_nums(ior_logs_path)

    # look for the line that starts with
    state = 'outside_tests'
    results = {}
    for line in lines:
        if state == 'outside_tests' and 'Summary of all tests' in line:
            state = 'in_summary'
        if state == 'in_summary' and line.startswith('Operation'):
            results['ops'] = line.split()
        if state == 'in_summary' and line.startswith('write'):
            results['write'] = line.split()
        if state == 'in_summary' and line.startswith('read'):
            results['read'] = line.split()
        if state == 'in_summary' and line.startswith('Finished'):
            break

    return results

def get_results(path, test_type):
    if test_type == 'ior':
        return get_data_from_ior(path)
    if test_type == 'mdtest':
        return get_data_from_mdtest(path)


def get_nodes_and_procs(path, test_type):
    '''Get the num nodes and num procs out of the log file'''
    lines = lines_without_proc_nums(path)

    if test_type == 'ior':
        for line in lines:
            if line.startswith('nodes'):
                nodes = int(line.split()[-1])
            if line.startswith('tasks'):
                procs = int(line.split()[-1])
                return nodes, procs

    if test_type == 'mdtest':
        # look for line with 'launched with'
        for line in lines:
            if 'launched with' in line:
                nums = []
                for token in line.split():
                    try:
                        nums.append(int(token))
                    except:
                        pass
                return nums[1], nums[0]

    # likely an error in the run, so no nodes/procs
    return None, None



def do_it_all():
    '''Do it all, go from files to to speadsheet summary
    '''

    meta_data = get_all_meta()

    # now get all the results, if no results, throw out the meta_data
    for x in meta_data:
        try:
            x['results'] = get_results(x['output_logs_path'], x['test_type'])
        except:
            pass

    meta_data = [x for x in meta_data if x.get('results')]

    ior = [x for x in meta_data if x['test_type'] == 'ior']
    mdtest = [x for x in meta_data if x['test_type'] == 'mdtest']

    # print mdtest
    processed_mdtest = []
    for mdt in mdtest:
        try:
            processed_mdtest.append(process_mdtest(mdt))
        except:
            pass

    #print(process_mdtest(None, print_labels=True))
    #for x in sorted(processed_mdtest):
    #    print(x)

    # print ior
    processed_ior = []
    for iort in ior:
        #try:
        processed_ior.append(process_ior(iort))
        #except:
        #    pass

    print(process_ior(None, print_labels=True))
    for x in sorted(processed_ior, key=operator.itemgetter(7,2,3,0,1)):
        print(','.join(x))

    return meta_data




ior_labels  = [
    'nodes',
    'procs',
    'jbod_mode',
    'zfs_dirty_data_max',
    'zfs_dirty_data_max_percent',
    'zfs_max_recordsize',
    'recordsize',
    'op',
    'read(MiB)',
    'read(OPs)',
    'write(MiB)',
    'write(OPs)',
]

mdtest_labels = [
    'nodes',
    'procs',
    'jbod_mode',
    'zfs_dirty_data_max',
    'zfs_dirty_data_max_percent',
    'zfs_max_recordsize',
    'recordsize',
    'file_creation',
    'file_read',
    'file_removal',
    'file_stat',
    'tree_creation',
    'tree_removal',
]

def process_ior(meta_data):
    labels = ior_labels

    m = meta_data
    data = [
        int(m['num_nodes']),
        int(m['num_procs']),
        int(m['jbod_zfs_params']['jbod_mode']),
        int(m['jbod_zfs_params']['zfs_dirty_data_max']),
        int(m['jbod_zfs_params']['zfs_dirty_data_max_percent']),
        int(m['jbod_zfs_params']['zfs_max_recordsize']),
        m['jbod_zfs_params']['recordsize'],
    ]
    if 'read' in m['results']:
        data.append('read')
            mib_index = m['results']['ops'].index('Mean(MiB)')
        ops_index = m['results']['ops'].index('Mean(OPs)')
        data.append(float(m['results']['read'][mib_index]))
        data.append(float(m['results']['read'][ops_index])))
        data.append(0)
        data.append(0)
    if 'write' in m['results']:
        data.append('write')
            data.append(0)
        data.append(0)
        mib_index = m['results']['ops'].index('Mean(MiB)')
        ops_index = m['results']['ops'].index('Mean(OPs)')
        data.append(float(m['results']['write'][mib_index]))
        data.append(float(m['results']['write'][ops_index]))

    return data


def process_mdtest(meta_data):

    m = meta_data
    data = [
        int(m['num_nodes']),
        int(m['num_procs']),
        int(m['jbod_zfs_params']['jbod_mode']),
        int(m['jbod_zfs_params']['zfs_dirty_data_max']),
        int(m['jbod_zfs_params']['zfs_dirty_data_max_percent']),
        int(m['jbod_zfs_params']['zfs_max_recordsize']),
        m['jbod_zfs_params']['recordsize'],
        float(m['results']['rate']['file_creation']['mean']),
        float(m['results']['rate']['file_read']['mean']),
        float(m['results']['rate']['file_removal']['mean']),
        float(m['results']['rate']['file_stat']['mean']),
        float(m['results']['rate']['tree_creation']['mean']),
        float(m['results']['rate']['tree_removal']['mean']),
    ]

    return data



def make_table(meta_data_list, earliest=None, test_type='mdtest',
               jbod_mode=0, zfs='default', op='create'):
    '''Create a dataframe of all the data.
    zfs can be default, brian, brian_10'''

    if test_type == 'mdtest':
        labels = mdtest_labels
        process_meta_data = process_mdtest
    if test_type == 'ior':
        labels = ior_labels
        process_meta_data = process_ior

    if zfs == 'default':
        zfs_params = default_zfs_params
    if zfs == 'brian':
        zfs_params = brian_zfs_params
    if zfs == 'brian_10':
        zfs_params = brian_10_zfs_params

    # filter on time
    if earliest is not None:
        meta_data_list = [
            md for md in meta_data_list if
            md['timestamp'] >= earliest
        ]

    # first filter on test_type
    meta_data_list = [
        md for md in meta_data_list if (
            (md['test_type'] == test_type)
            and
            (int(md['jbod_zfs_params']['jbod_mode']) == jbod_mode)
            and
            # subset (could fail on string vs int)
            (zfs_params.items() <=  md['jbod_zfs_params'].items())
            and
            (md['test_subtype'] == op)
        )
    ]

    data = []
    for md in meta_data_list:
        data.append(process_meta_data(md))

    return pandas.DataFrame(data=data, columns=labels)
