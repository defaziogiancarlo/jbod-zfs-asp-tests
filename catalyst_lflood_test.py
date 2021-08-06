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

import yaml

# TODO make it so script can run on opal


ior_logs_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/ior_logs'
)
mdtest_logs_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/mdtest_logs'
)
scripts_dir = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/srun_commands'
)
stonewall_status = pathlib.Path(
    '/g/g0/defazio1/non-jira-projects/jbod-zfs-asp-tests/stonewall_status'
)

#if 'catalyst' in os.environ['HOSTNAME']:
mdtest_files_path = '/p/lflood/defazio1/io500-all/mdtest-easy-cleanup'
ior_files_path = ''
partition = ['-p', 'pgarter']
#elif 'opal' in os.environ['HOSTNAME']:
#    mdtest_files_path = '/p/lquake/defazio1/io500-all'
#    ior_files_path = ''
#    partition = []

mdtest_path = pathlib.Path('/g/g0/defazio1/repos/ior/src/mdtest')

ior_path = pathlib.Path('/g/g0/defazio1/repos/ior/src/ior')

cameron_mdtest_create_flags = [
    str(mdtest_path),
    '-n', '1000000', '-u', '-L', '-F', '-P', '-N', '1',
    '-d', str(mdtest_files_path),
    '-x', str(stonewall_status),
    '-C', '-Y', '-W', '300', '-a', 'POSIX',
]

cameron_mdtest_stat_flags = [
    str(mdtest_path),
    '-n', '1000000', '-u', '-L', '-F', '-P', '-N', '1',
    '-d', str(mdtest_files_path),
    '-x', str(stonewall_status),
    '-T', '-a', 'POSIX',
]

def make_mdtest_command_from_template(timestamp, template):
    '''Make a command that can be run and will store it's data in the
    right place. The template can omit the location of where the test files
    will be created (after flag -d) and where the stonewall logs will go
    (after flag -x)
    '''
    command = copy.deepcopy(template)
    command.insert(0, str(mdtest_path))
    command.insert(str(mdtest_files_path), command.index('-d')+1)
    command.insert(str(mdtest_files_path / timestamp), command.index('-x')+1)

    return command

def make_mdtest_command(timestamp, command=None):
    if command is None:
        command = [
            str(mdtest_path),
            '-v', # verbose
            # TODO should be -n 1000000
            '-n', '100', # number of files per process
            '-u',
            '-L',
            '-F',
            '-P',
            '-N', '1',
            '-d', str(mdtest_files_path),
            #'-x', '/g/g0/defazio1/mdtest_results/mdtest-easy-cleanup.stonewall',
            '-x', str(stonewall_status / timestamp),
            '-Y',
            '-W', '300',
            '-a', 'POSIX',
        ]
    #command += optional_flags
    # format command into a string with each arugument in single quotes
    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command

def make_mdtest_command(timestamp, command=None):
    if command is None:
        command = [
            str(mdtest_path),
            '-v',
            '-n', '1000000',
            '-u',
            '-L',
            '-F',
            '-P',
            '-N', '1',
            '-d', str(mdtest_files_path),
            '-x', str(stonewall_status / timestamp),
            '-T',
            '-C',
            '-Y',
            '-W', '300',
            '-a', 'POSIX',
        ]
    command = ['\'' + x + '\'' for x in command]
    command = ' '.join(command)
    return command



def make_ior_command(timestamp):
    command = [
        str(ior_path),
        '-C',
        '-Q', '1',
        '-g',
        '-G', '271',
        '-k',
        '-e',
        '-o', '/p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy/ior_file_easy',
        '-O', 'stoneWallingStatusFile=./results/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy.stonewall',
        '-t', '2m'
        '-b', '9920000m'
        '-F',
        '-w',
        '-D', '300'
        '-O', 'stoneWallingWearOut=1',
        '-a', 'POSIX',
    ]

def make_srun_command(test_command_path, num_nodes=None, num_procs=None): # , nodes_and_procs):
    #num_nodes, num_procs = get_nodes_and_procs()
    # pgarter is specifically for catalyst
    #num_nodes, num_procs = nodes_and_procs


    cmd = ['srun'] + partition + [f'-N{num_nodes}',
                                  f'-n{num_procs}',
                                  '-l',
                                  str(test_command_path)]
    return cmd


def single_srun(test_type='mdtest', jbod_zfs_params=None, dryrun=False,
                num_nodes=None, num_procs=None):
    '''Create a file with a ior or mdtest command, then run it.
    Test type is either 'mdtest' or 'ior'
    jbod_zfs_params is a string of the jbod and zfs commands
    used on the server side, if included it will be in the script
    commented out
    '''

    if test_type.lower() == 'mdtest':
        make_command = make_mdtest_command
        logs_dir = mdtest_logs_dir
    elif test_type.lower() == 'ior':
        make_command = make_ior_test
        logs_dir = ior_logs_dir


    #optional_flags = get_optional_flags(all_optional_flags)
    timestamp = str(datetime.datetime.now()).replace(' ', '_').replace(':', '')

    command = make_command(
        timestamp,
        #optional_flags=optional_flags
    )
    script_path = scripts_dir / timestamp
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
    if not dryrun:
        srun_output = subprocess.run(
            srun_command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout.decode()
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



def main():
    parser = make_parser()
    args = vars(parser.parse_args())
    print(args)


    if args['iterate']:
        for num_nodes_base in range(6):
            for procs_per_node_base in range(5):
                num_nodes = 2**num_nodes_base
                procs_per_node =2** procs_per_node_base
                num_procs = num_nodes * procs_per_node
                single_srun(
                    dryrun=args['dryrun'],
                    jbod_zfs_params=args['zfs_params'],
                    num_nodes=num_nodes,
                    num_procs=num_procs,
                )

    else:
        single_srun(dryrun=args['dryrun'], jbod_zfs_params=args['zfs_params'])



if __name__ == '__main__':
    # if len(sys.argv) > 1:
    #     runs = int(sys.argv[1])
    # else:
    #     runs = 3
    # for _ in range(runs):
    #     single_srun()



    main()
