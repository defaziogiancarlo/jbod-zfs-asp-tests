'''Summarize the data.'''

import ast
import glob
import pathlib
import re

import yaml

# plan to get all meta_data
# look at all the srun_files
# create a dict and a filename from each, write to
# temp_meta_data_dir


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

meta_data_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/test_meta_data'
)

temp_meta_data_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/temp_test_meta_data'
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
    match = False
    for line in lines:
        m = zfs_line_pattern.match(line)
        if m:
            match = True
      #      print("match")
            zfs_line = m.group()
        #else:
        #    return None
    if not match:
        return None

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

def get_all_run_meta_data():
    '''Read in all the files in the runs meta_data directory, and put them into
    a list.
    '''
    readme = {'README.md', 'README.md~'}
    # get all files, watch out for READMEs
    paths = meta_data_dir.glob('*')
    paths = [
        path for
        path in paths
        if
        ((path.name not in readme) and (not path.name.endswith('md')))
    ]
    meta_data = []
    for path in paths:
        with open(path, 'r') as f:
            meta_data.append(yaml.safe_load(f))

    return meta_data



def make_meta_for_existing():
    '''Make meta_data files for existing runs.
    find the exising sruns
    find the existing meta_data files
    find the existing logs files

    for the sruns without meta_data
    create a meta_data file




    '''
    readme = {'README.md', 'README.md~'}
    # get all the srun files:
    srun_files = srun_commands_dir.glob('*')
    srun_files = set([x.name for x in srun_files]) - readme

    meta_data_files = meta_data_dir.glob('*')
    meta_data_files = set([x.name for x in  meta_data_files]) - readme

    srun_files = srun_files - meta_data_files
    # if no meta_data file, create a temp meta_data file

    iors, mdtests = completed_runs()
    # check if output_logs, otherwise dryrun is true



    for srun_file in srun_files:
        path = temp_meta_data_dir / srun_file

        zfs_stuff = parse_zfs_params(srun_commands_dir / srun_file)


        meta_data =  {
            'command': None,
            'srun_command': None,
            'script_path': None,
            'output_logs_path': None,
            'jbod_zfs_params': zfs_stuff,
            'num_nodes': 1,
            'num_procs': 1,
            'dryrun': (
                srun_file not in iors and srun_file not in mdtests
            ),
            'test_type': None,
            'timestamp': srun_file,
        }
        with open(path, 'w') as f:
            yaml.safe_dump(meta_data, f)



def make_test_meta_data(command, srun_command, output_logs_path,
                        jbod_zfs_params, test_type, timestamp,
                        num_nodes, num_procs, dryrun, template,
                        script_path):
    '''log all pertinent data about a test.
    dry_run
    ior vs. mdtest
    if ior
      read write
    if mdtest
      stat create
    output files
    test command
    srun command
    num_modes
    num_procs
    jbod mode
    zfs_params
    '''
    meta_data =  {
        'command': command,
        'srun_command': srun_command,
        'script_path': str(script_path),
        'output_logs_path': str(output_logs_path),
        'jbod_zfs_params': jbod_zfs_params,
        'num_nodes': num_nodes,
        'num_procs': num_procs,
        'dryrun': dryrun,
        'test_type': test_type,
        'timestamp': timestamp,
    }
    # now attemp to figure out if it's create, stat, write, or read
    if template == cameron_mdtest_create_template:
        test_subtype = 'create'
    elif template == cameron_mdtest_stat_template:
        test_subtype = 'stat'
    elif template == cameron_ior_read_template:
        test_subtype = 'read'
    elif template == cameron_ior_write_template:
        test_subtype = 'write'

    meta_data['test_subtype'] = test_subtype

    return meta_data
