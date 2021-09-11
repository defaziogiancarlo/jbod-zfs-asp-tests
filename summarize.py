'''Summarize the data.'''

import ast
import glob
import operator
import pathlib
import pprint
import re

import yaml

# plan to get all meta_data
# look at all the srun_files
# create a dict and a filename from each, write to
# temp_meta_data_dir

srun_proc_prefix = re.compile(r'^\s*\d+: ')

def remove_proc_number(line):
    return srun_proc_prefix.sub('', line, count=1)


def lines_without_proc_nums(path):
    with open(path, 'r') as f:
        return [remove_proc_number(line) for line in f]


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

def get_data_from_ior(ior_logs_path):
    '''Parse the output of ior and get the data
    you crave!
    '''

    with open(ior_logs_path, 'r') as f:
        lines = f.read().splitlines()
    lines = [remove_proc_number(line) for line in lines]

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

def get_command(path):
    '''get the command from the srun file'''
    with open(path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        if lines != '' and not line.startswith('#'):
            cmd = line.split()
            cmd = [x[1:-1] for x in cmd]
            return cmd


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

    files_to_do = srun_files & (iors | mdtests)


    for srun_file in files_to_do:

        # timestamp is the filename
        timestamp = srun_file

        path = temp_meta_data_dir / timestamp

        srun_path = srun_commands_dir / timestamp
        zfs_stuff = parse_zfs_params(srun_path)

        # get the command (mdtest or ior)
        command = get_command(srun_path)
        if command is None:
            # probably a super dry run
            continue

        # get ior/mdetest from command
        test_type = pathlib.Path(command[0]).name

        # logs path should just be the test type and the timestamp
        if test_type == 'ior':
            output_logs_path = ior_logs_dir / timestamp
        if test_type == 'mdtest':
            output_logs_path = mdtest_logs_dir / timestamp

        # based on the test type, get num_node and num_procs
        num_nodes, num_procs = get_nodes_and_procs(output_logs_path, test_type)
        if num_nodes is None or num_procs is None:
            continue


        meta_data =  {
            'command': command,
            'srun_command': None,
            'script_path': str(srun_path),
            'output_logs_path': str(output_logs_path),
            'jbod_zfs_params': zfs_stuff,
            'num_nodes': num_nodes,
            'num_procs': num_procs,
            'dryrun': (
                srun_file not in iors and srun_file not in mdtests
            ),
            'test_type': test_type,
            'timestamp': timestamp,
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

def get_all_meta():

    # get all the paths for the runs to analyze
    #completed_ior, completed_mdtest  = completed_runs()
    #completed_ior = [ior_logs_dir / x for x in completed_ior]
    #completed_mdtest= [mdtest_logs_dir / x for x in completed_mdtest]

    #

    meta_logs = set(meta_data_dir.glob('*')) - {'README.md', 'README.md~'}
    temp_meta_logs = set(temp_meta_data_dir.glob('*')) - {'README.md', 'README.md~'}

    meta_data_logs = meta_logs | temp_meta_logs

    # make list of meta_data
    meta_data = []
    for path in meta_data_logs:
        with open(path, 'r') as f:
            meta_data.append(yaml.safe_load(f))

    for x in meta_data:
        if isinstance(x['jbod_zfs_params'], str):
            x['jbod_zfs_params'] = ast.literal_eval(x['jbod_zfs_params'])

    return meta_data


def process_ior(meta_data, print_labels=False):
    labels  = [
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
    if print_labels:
        return ','.join(labels)

    m = meta_data
    #print(m)
    #print(type(m['jbod_zfs_params']))
    data = [
        m['num_nodes'],
        m['num_procs'],
        m['jbod_zfs_params']['jbod_mode'],
        m['jbod_zfs_params']['zfs_dirty_data_max'],
        m['jbod_zfs_params']['zfs_dirty_data_max_percent'],
        m['jbod_zfs_params']['zfs_max_recordsize'],
        m['jbod_zfs_params']['recordsize'],
    ]
    if 'read' in m['results']:
        data.append('read')
        #print(m['results']['ops'])
        mib_index = m['results']['ops'].index('Mean(MiB)')
        ops_index = m['results']['ops'].index('Mean(OPs)')
        data.append(m['results']['read'][mib_index])
        data.append(m['results']['read'][ops_index])
        data.append('N/A')
        data.append('N/A')
    if 'write' in m['results']:
        data.append('write')
        #print(m['results']['ops'])
        data.append('N/A')
        data.append('N/A')
        mib_index = m['results']['ops'].index('Mean(MiB)')
        ops_index = m['results']['ops'].index('Mean(OPs)')
        data.append(m['results']['write'][mib_index])
        data.append(m['results']['write'][ops_index])



    data = [str(x) for x in data]
    #data = sorted(data, key=operator.itemgetter(2,0,1))
    #s = ','.join(data)
    #print(s)
    return data


def process_mdtest(meta_data, print_labels=False):
    labels  = [
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
    if print_labels:
        return ','.join(labels)


    m = meta_data
    #print(m)
    #print(type(m['jbod_zfs_params']))
    data = [
        m['num_nodes'],
        m['num_procs'],
        m['jbod_zfs_params']['jbod_mode'],
        m['jbod_zfs_params']['zfs_dirty_data_max'],
        m['jbod_zfs_params']['zfs_dirty_data_max_percent'],
        m['jbod_zfs_params']['zfs_max_recordsize'],
        m['jbod_zfs_params']['recordsize'],
        m['results']['rate']['file_creation']['mean'],
        m['results']['rate']['file_read']['mean'],
        m['results']['rate']['file_removal']['mean'],
        m['results']['rate']['file_stat']['mean'],
        m['results']['rate']['tree_creation']['mean'],
        m['results']['rate']['tree_removal']['mean'],
    ]
    data = [str(x) for x in data]
    s = ','.join(data)
    #print(s)
    return s
