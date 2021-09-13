'''A script to run a bunch of asp performance tests on lflood via
catalyst.
'''

import argparse
import copy
import datetime
import os
import pathlib
import pprint
import random
import subprocess
import sys
import time

import yaml


# TODO make this not suck
# have arg parser, be able to specify a bunch of tests
# and have it just go
# record stuff in resonable, searchable way
# pull the restuls of out the logs

# be able to read in config file for:
# machine it's on
# ior and mdtest params
# batch to use
# number of node, and processes per node

# TODO see why HOSTNAME fails for root


# TODO make it so script can run on opal

# maybe get rid of globals, just have a dict
# that get set by get_config, or has defaults

# the cameron values might get renamed or moved
# kept here for now

# TODO should this be a templates dict
# with each template as a member? yes
cameron_mdtest_create_template = [
    '-n', '1000000',
    '-u', '-L', '-F', '-P',
    '-N', '1',
    '-d',
    '-x',
    #'-C', # as far as I know, including -C this prevents cleanup
    '-Y',
    '-W', '300',
    '-a', 'POSIX',
]
cameron_mdtest_stat_template = [
    '-n', '1000000',
    '-u', '-L', '-F', '-P',
    '-N', '1',
    '-d',
    '-x',
    '-T',
    '-a', 'POSIX',
]

cameron_ior_read_template = [
    '-C',
    '-Q', '1',
    '-g',
    '-G', '271',
    '-k', '-e',
    '-o',
    '-O',
    '-t', '2m',
    '-b', '9920000m',
    '-F', '-r', '-R',
    '-a', 'POSIX',
]

cameron_ior_write_template = [
    '-C',
    '-Q', '1',
    '-g',
    '-G', '271',
    '-k', '-e',
    '-o',
    '-O',
    '-t', '2m',
    '-b', '9920000m',
    '-F', '-w',
    '-D', '300',
    '-O', 'stoneWallingWearOut=1',
    '-a', 'POSIX',
]

def get_hostname():
    '''For some reason root does have HOSTNAME in env,
    so you the hostname utility instead.
    '''
    return subprocess.run(
        ['hostname'],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.decode().strip()

def get_config(config_file_path):
    '''Read in a config file, and set the globals to it
    values.
    '''
    with open(config_file_path, 'r') as f:
        return yaml.safe_load(f)

def get_config_values(args):
    '''Get the configuration values. These come from either the defaults
    or from a config file. Also, some can be specified on the command
    line, but that happens external to this function.
    '''
    # get the default values
    defaults = {}

    # set the hostname, which is used for other value
    defaults['hostname'] = get_hostname()

    defaults['ior_logs_dir'] = pathlib.Path(
        '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/ior_logs'
    )
    defaults['mdtest_logs_dir'] = pathlib.Path(
        '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/mdtest_logs'
    )
    defaults['scripts_dir'] = pathlib.Path(
        '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/srun_commands'
    )
    defaults['stonewall_status'] = pathlib.Path(
        '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/stonewall_status'
    )
    defaults['stonewall_status_ior'] = pathlib.Path(
        '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/stonewall_status_ior'
    )
    defaults['meta_data_dir'] = pathlib.Path(
        '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/test_meta_data'
    )


    if 'catalyst' in defaults['hostname']:
        defaults['mdtest_files_path'] = pathlib.Path(
            '/p/lflood/defazio1/io500-all/mdtest-easy-cleanup'
        )
        defaults['ior_files_path'] = pathlib.Path(
            '/p/lflood/defazio1/io500-all/ior/'
        )
        defaults['partition'] = ['-p', 'pgarter']
    elif 'opal' in defaults['hostname']:
        defaults['mdtest_files_path'] = pathlib.Path(
            '/p/lquake/defazio1/io500-all'
        )
        defaults['ior_files_path'] = ''
        defaults['partition'] = []

    defaults['mdtest_path'] = pathlib.Path(
        '/g/g0/defazio1/repos/ior/src/mdtest'
    )

    defaults['ior_path'] = pathlib.Path(
        '/g/g0/defazio1/repos/ior/src/ior'
    )

    config_file_path = args.get('config')
    if config_file_path is not None:
        defaults.update(get_config())

    args_filtered = {k:v for k,v in args.items() if k in defaults}
    # add any matching keys from args


    # get the value from the config file and update
    defaults.update(args_filtered)

    return defaults



def make_mdtest_command_from_template(timestamp, template, config, stone_ts=None):
    '''Make a command that can be run and will store it's data in the
    right place. The template can omit the location of where the test files
    will be created (after flag -d) and where the stonewall logs will go
    (after flag -x)
    '''
    command = copy.deepcopy(template)
    command.insert(0, str(config['mdtest_path']))
    if '-v' not in command:
        command.insert(1, '-v')
    command.insert(command.index('-d') + 1, str(config['mdtest_files_path']))
    command.insert(command.index('-x') + 1, str(config['stonewall_status'] / timestamp))

    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command



def make_ior_command_from_template(timestamp, template, config, stone_ts=None):
    if stone_ts is None:
        stone_ts = timestamp
    command = copy.deepcopy(template)
    command.insert(0, str(config['ior_path']))
    command.insert(command.index('-o')+1, str(config['ior_files_path']))
    command.insert(
        command.index('-O') + 1,
        ('stoneWallingStatusFile='+ str(config['stonewall_status_ior'] / stone_ts))
    )

    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command



def make_mdtest_command(timestamp, config, command=None):
    if command is None:
        command = [
            str(config['mdtest_path']),
            '-v', # verbose
            # TODO should be -n 1000000
            '-n', '100', # number of files per process
            '-u',
            '-L',
            '-F',
            '-P',
            '-N', '1',
            '-d', str(config['mdtest_files_path']),
            #'-x', '/g/g0/defazio1/mdtest_results/mdtest-easy-cleanup.stonewall',
            '-x', str(config['stonewall_status'] / timestamp),
            '-Y',
            '-W', '300',
            '-a', 'POSIX',
        ]
    #command += optional_flags
    # format command into a string with each arugument in single quotes
    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command

def make_mdtest_command(timestamp, config, command=None):
    if command is None:
        command = [
            str(config['mdtest_path']),
            '-v',
            '-n', '1000000',
            '-u',
            '-L',
            '-F',
            '-P',
            '-N', '1',
            '-d', str(config['mdtest_files_path']),
            '-x', str(config['stonewall_status'] / timestamp),
            #'-T',
            '-C',
            '-Y',
            '-W', '300',
            '-a', 'POSIX',
        ]
    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command



def make_ior_command(timestamp, config, template=None):
    command = [
        str(config['ior_path']),
        '-C',
        '-Q', '1',
        '-g',
        '-G', '271',
        '-k',
        '-e',
        '-o', str(config['ior_files_path']),
        '-O', ('stoneWallingStatusFile='+ str(config['stonewall_status_ior'] / timestamp)),
        '-t', '2m',
        '-b', '9920000m',
        '-F',
        '-w',
        '-D', '300',
        '-O', 'stoneWallingWearOut=1',
        '-a', 'POSIX',
    ]
    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command

def make_ior_command(timestamp, config, template=None):
    return cameron_mdtest_create_template(timestamp, template, config)




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


def log_test_meta_data(meta_data, config):
    '''log the test_meta_data'''
    path = config['meta_data_dir'] / meta_data['timestamp']
    with open(path, 'w') as f:
        yaml.safe_dump(meta_data, f)

def make_srun_command(test_command_path, config, num_nodes=None, num_procs=None): # , nodes_and_procs):
    #num_nodes, num_procs = get_nodes_and_procs()
    # pgarter is specifically for catalyst
    #num_nodes, num_procs = 4,4

    cmd = ['srun'] + config['partition'] + [f'-N{num_nodes}',
                                            f'-n{num_procs}',
                                            '--time', '05:59:00',
                                            '-l',
                                            str(test_command_path)]
    return cmd






def single_srun(config, test_type='mdtest', template=None, jbod_zfs_params=None, dryrun=False,
                num_nodes=None, num_procs=None, tries=5, ts=None, stone_ts=None):
    '''Create a file with a ior or mdtest command, then run it.
    Test type is either 'mdtest' or 'ior'
    jbod_zfs_params is a string of the jbod and zfs commands
    used on the server side, if included it will be in the script
    commented out
    '''

    if test_type.lower() == 'mdtest':
        # make_command = make_mdtest_command
        make_command = make_mdtest_command_from_template
        logs_dir = config['mdtest_logs_dir']
    elif test_type.lower() == 'ior':
        # make_command = make_ior_command
        make_command = make_ior_command_from_template
        logs_dir = config['ior_logs_dir']


    #optional_flags = get_optional_flags(all_optional_flags)
    if ts is None:
        timestamp = str(datetime.datetime.now()).replace(' ', '_').replace(':', '')
    else:
        timestamp = ts


    command = make_command(
        timestamp,
        template,
        config,
        stone_ts=stone_ts,

        #optional_flags=optional_flags
    )
    script_path = config['scripts_dir'] / timestamp
    with open(script_path, 'w') as f:
        if dryrun:
            f.write('# DRYRUN, this script will not be executed')
        f.write('#!/bin/bash\n')
        f.write('# autogenerated by catalyst_lflood_test.py\n')
        f.write(command)
        f.write('\n')
        if jbod_zfs_params is not None:
            f.write('# ' + str(jbod_zfs_params) + '\n')
    script_path.chmod(0o755)
    srun_command = make_srun_command(script_path,
                                     config,
                                     num_nodes=num_nodes,
                                     num_procs=num_procs)
    output_logs_path = logs_dir / timestamp
    run_data = {
        'command': command,
        'srun_command': srun_command,
        'output_logs_path': output_logs_path,
        'jbod_zfs_params': jbod_zfs_params,
    }
    # TODO you could just logs this data somewhere, it has
    # what you need to do analysis
    print(run_data)
    print()
    test_meta_data = make_test_meta_data(
        command, srun_command, output_logs_path,
        jbod_zfs_params, test_type, timestamp,
        num_nodes, num_procs, dryrun, template,
        script_path,
    )
    log_test_meta_data(test_meta_data, config)

    if not dryrun:

        # slurm keeps kicking me out, so retry as needed
        for t in range(1,tries+1):
            try:
                srun_output = subprocess.run(
                    srun_command,
                    check=True,
                    stdout=subprocess.PIPE,
                    #stderr=subprocess.STDOUT,
                ).stdout.decode()
                break
            except subprocess.CalledProcessError:
                print(f'failed try {t} of {tries}, retrying in {t} minutes...')
                time.sleep(60 * t)
                print('trying again')

        with open(output_logs_path, 'w+') as f:
            f.write(srun_output)

# /g/g0/defazio1/ior/src/mdtest '-v' '-n' '1000000' '-u' '-L' '-F' '-P' '-N' '1' '-d' '/p/lflood/defazio1/io500-all/mdtest-easy-cleanup' '-x' '/g/g0/defazio1/mdtest_results/mdtest-easy-cleanup.stonewall' '-Y' '-W' '300' '-a' 'POSIX'

def make_parser():
    description = 'do some tests'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        '--num-test-runs',
        help='The number of test runs to perform'
    )
    parser.add_argument(
        '--test-type',
        default='mdtest',
        help='the type of test, either \'mdtest\' or \'ior\''
    )
    parser.add_argument(
        '-c',
        '--command',
        help='a ior or mdtest command to run',
    )
    parser.add_argument(
        '-p',
        '--command-path',
        help='the path a file with a ior or mdtest command',
    )
    parser.add_argument(
        '-d',
        '--dryrun',
        action='store_true',
        help='don\'t actually run the test',
    )
    parser.add_argument(
        '-z',
        '--zfs-params',
        help=(
            'these are the zfs params being used in the server side, '
            'this is just a way of recording them'
        )
    )
    parser.add_argument(
        '-i',
        '--iterate',
        action='store_true',
        help='iterate over the number of procs and nodes',
    )
    return parser


#def iter_nodes_procs()

def mdtest_ior(args, config, num_nodes, num_procs):
    mdtest_create_stat(args, config, num_nodes, num_procs)
    ior_write_read(args, config, num_nodes, num_procs)

def mdtest_create_stat(args, config, num_nodes, num_procs):

    ts = timestamp = str(datetime.datetime.now()).replace(' ', '_').replace(':', '')
    single_srun(
        config,
        template = cameron_mdtest_create_template,
        dryrun=args['dryrun'],
        jbod_zfs_params=args['zfs_params'],
        test_type='mdtest',
        ts=ts,
        num_nodes=num_nodes,
        num_procs=num_procs,
    )
    # TODO figure out why stat test fails
    # single_srun(
    #     config,
    #     template = cameron_mdtest_stat_template,
    #     dryrun=args['dryrun'],
    #     jbod_zfs_params=args['zfs_params'],
    #     test_type='mdtest',
    #     #ts=ts,
    #     stone_ts=ts,
    #     num_nodes=num_nodes,
    #     num_procs=num_procs,
    # )


def ior_write_read(args, config, num_nodes, num_procs):
    '''do an ior write followed by an ior read'''
    ts = timestamp = str(datetime.datetime.now()).replace(' ', '_').replace(':', '')
    single_srun(
        config,
        template = cameron_ior_write_template,
        dryrun=args['dryrun'],
        jbod_zfs_params=args['zfs_params'],
        test_type='ior',
        ts=ts,
        num_nodes=num_nodes,
        num_procs=num_procs,
    )
    single_srun(
        config,
        template = cameron_ior_read_template,
        dryrun=args['dryrun'],
        jbod_zfs_params=args['zfs_params'],
        test_type='ior',
        #ts=ts,
        stone_ts=ts,
        num_nodes=num_nodes,
        num_procs=num_procs,
    )

def zfs_jbod_all(args, config):
    '''For a given zfs/jbod param combo,
    do all the testing for mdtest and ior
    and store all the data.
    '''
    for num_nodes in [1,2,4,8,16,32]:
        for procs_per_node in [1,2,4,8,16]:
            if num_nodes == 1 and procs_per_node < 4:
                continue
    # for num_nodes in [1]:
    #     for procs_per_node in [1]:

            num_procs = num_nodes * procs_per_node
            mdtest_create_stat(args, config, num_nodes, num_procs)
            ior_write_read(args, config, num_nodes, num_procs)



def main():
    parser = make_parser()
    args = vars(parser.parse_args())
    config = get_config_values(args)

    #print(config)
    #sys.exit(1)
    #print(args)


    if args['iterate']:
        for num_nodes in [2,4,8,16,32]:
            for procs_per_node in [1,2,4,8,16]:
                # num_procs = num_nodes * procs_per_node
                # single_srun(
                #     config,
                #     template = cameron_mdtest_create_template,
                #     dryrun=args['dryrun'],
                #     jbod_zfs_params=args['zfs_params'],
                #     test_type='mdtest',
                #     # ts=ts,
                #     num_nodes=num_nodes,
                #     num_procs=num_procs,
                # )

                if (num_nodes == 2 and procs_per_node < 8):
                    continue
                #(num_nodes == 2 and procs_per_node == 2)):
                #    continue
                else:
                    num_procs = num_nodes * procs_per_node
                    ior_write_read(args, config, num_nodes, num_procs)
                #mdtest_create_stat(args, config, num_nodes, num_procs)
    #             #num_nodes = 2**num_nodes_base
    #             #procs_per_node =2** procs_per_node_base
    #             num_procs = num_nodes * procs_per_node
    #             single_srun(
    #                 config,
    #                 template = cameron_ior_read_template,
    #                 dryrun=args['dryrun'],
    #                 jbod_zfs_params=args['zfs_params'],
    #                 num_nodes=num_nodes,
    #                 num_procs=num_procs,
    #                 test_type=args['test_type'],
    #             )
    # else:
    #     single_srun(
    #         config,
    #         template = cameron_ior_write_template,
    #         dryrun=args['dryrun'],
    #         jbod_zfs_params=args['zfs_params'],
    #         test_type=args['test_type'],
    #         num_nodes=2,
    #         num_procs=2,
    #     )



if __name__ == '__main__':
    # if len(sys.argv) > 1:
    #     runs = int(sys.argv[1])
    # else:
    #     runs = 3
    # for _ in range(runs):
    #     single_srun()
    # config = get_config_values({})
    # ts = str(datetime.datetime.now()).replace(' ', '_').replace(':', '')
    # md = make_mdtest_command(ts, config)
    # mdt = make_mdtest_command_from_template(ts, cameron_mdtest_create_template, config)
    # print(md == mdt)

    # i = make_ior_command(ts, config)
    # itt = make_ior_command_from_template(ts, cameron_ior_write_template, config)
    # print(i == itt)
    # print(make_ior_command_from_template(ts, cameron_ior_read_template, config))

    parser = make_parser()
    args = vars(parser.parse_args())
    config = get_config_values(args)

    zfs_jbod_all(args, config)
    #main()
