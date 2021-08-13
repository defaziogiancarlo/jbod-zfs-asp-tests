'''Summarize the data.'''

import ast
import glob
import pathlib
import re

zfs_line_pattern = r'^# {.*}$'
zfs_line_pattern = re.compile(zfs_line_pattern)

ior_logs_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/ior_logs'
)
mdtest_logs_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/mdtest_logs'
)
srun_commands_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/srun_commands'
)


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

# TODO make it so that dry run scripts get this label upon creation


def label_dry_runs():
    '''Look at the ouptut logs, and for the scripts that didn't run
    get all the file names in ior_logs_dir and mdtest_logs_dir
    get all the names in srun_commands
    any file in srun_commands without a corresponding file
    in the logs is considered dry run or incomplete run
    '''
    ior_logs_files = ior_logs_dir.glob('*')
    ior_logs_files = set([x.name for x in ior_logs_files])
    mdtest_logs_files = mdtest_logs_dir.glob('*')
    mdtest_logs_files = set([x.name for x in mdtest_logs_files])
    srun_files = srun_commands_dir.glob('*')
    srun_files = set([x.name for x in srun_files])

    all_log_files = ior_logs_files | mdtest_logs_files

    dry_runs = srun_files - all_log_files

    return dry_runs

def completed_runs():
    '''Look at the ouptut logs, and for the scripts that didn't run
    get all the file names in ior_logs_dir and mdtest_logs_dir
    get all the names in srun_commands
    any file in srun_commands without a corresponding file
    in the logs is considered dry run or incomplete run
    '''

    # the read me files
    readme = {'README.md', 'README.md~'}

    ior_logs_files = ior_logs_dir.glob('*')
    ior_logs_files = set([x.name for x in ior_logs_files]) - readme
    mdtest_logs_files = mdtest_logs_dir.glob('*')
    mdtest_logs_files = set([x.name for x in mdtest_logs_files]) - readme
    srun_files = srun_commands_dir.glob('*')
    srun_files = set([x.name for x in srun_files]) - readme

    completed_ior = srun_files & ior_logs_files
    completed_mdtest = srun_files & mdtest_logs_files

    return completed_ior, completed_mdtest




def generate_data():
    pass

def group_files(jbod, test_program, test_type, zfs_params):
    '''Go through the files, find the ones with certain parameters,
    and return a list of file names that match

    Look for:
      zfs params: default or Brian
      jbod: 0 or 2
      test_type: mdtest_creat, mdtest_stat, ior_write, ior_read
      nodes: 1,2,4,8,16,32
      total processes: 1,2,4,8,16,32,64,128,256,(512 maybe)
    '''
    # first, look at the mdtest and ior logs, to see what actually
    # ran, looking in srun commands includes dry runs

    # then look at the zfs and jbod params in the srun command files

    # then look at the num_procs, num_processes in the
    pass
