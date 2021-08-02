'''Set Params of the nodes for zfs and for jbod.
outputs a timestamped file that has the parameters.
This is useful for comparing with the ior/mdtest stuff
that you will do on catalyst. It might resolve some confusions
chronologically.
'''

import os
import re
import subprocess
import sys


if os.environ['HOSTNAME'] == 'garteri':
    lustre_nodes = 'egarter[5-8]'
elif os.environ['HOSTNAME'] == 'slagi':
    lustre_nodes = 'eslag[3-4]'

zfs_params_command = (
    'pdsh -w egarter[1-8] \"echo -n \'{} \' && '
    'cat /sys/module/zfs/parameters/{}\"'
)

def append_value(key, value, d=None):
    '''
    Utility function for a dict of lists.
    append value to the end of list if key exists,
    or create the list and add value if key does exist.
    mutates the dict and returns it.
    If no dict given, or None given, creates a new dict.
    '''
    # if dict is
    if d is None:
        d = {}

    if key in d:
        d[key].append(value)
    else:
        d[key] = [value]

    return d


def organize_pdsh(text):
    '''Organize the output from pdsh
    by grouping b host and removing the hostname
    from the beginning of each line.
    expect the stdout from pdsh'''

    # turn the output into the proper types
    # split it up and get rid of empty strings
    text = text.decode().split('\n')
    text = [line for line in text if line != '']

    grouped_by_host = {}
    pdsh_line = r'(?P<host>^\w+): (?P<output>.*)'
    pdsh_line_re = re.compile(pdsh_line)

    for line in text:
        host, output = pdsh_line_re.match(line).groups()
        append_value(host, output, grouped_by_host)

    return grouped_by_host


def capture_zfs_param(param_name):
    '''Capture the values of the zfs prameter for
    all nodes.
    '''
    cmd = f'pdsh -w {lustre_nodes} cat /sys/module/zfs/parameters/{param_name}'
    text = subprocess.run(
        [cmd],
        check=True,
        shell=True,
        stdout=subprocess.PIPE,
    ).stdout

    host_to_value = organize_pdsh(text)

    # convert from {host:[value]} to {host: {param_name:value}}
    # there should be only one value in the value list

    host_to_param_value = (
        {host: {param_name: value[0]} for host,value in host_to_value.items()}
    )

    return host_to_param_value
    # now parse the ouptut,
    # the returned value will be
    # {garter<n> : {param_name, param_value}}


def capture_zfs_params(param_list):
    '''For each param in the list, get the param,
    and combine everthign into a single dict of form
    {host: {}}

    '''
    capture_params = [capture_zfs_param(param) for param in param_list]
    # now combine the dicts
    host_to_params = {}
    # keys should be identical for each member of capture_params
    keys = list(capture_params[0].keys())
    for key in keys:
        host_to_params[key] = {}
        for param_group in capture_params:
            host_to_params[key].update(param_group[key])

    return host_to_params


def set_zfs_param(param_name, value):
    '''For all the hosts, set the zfs param to the value.'''
    cmd = (
        f'pdsh -w {lustre_nodes} '
        f'echo {value} /sys/module/zfs/parameters/{param_name}'
    )
    subprocess.run(
        [cmd],
        check=True
    )

def set_zfs_params(param_to_values):
    '''Set all the nodes to have the given param values.'''
    # create a check dict, as in the dict you expect to get from
    # capture_zfs_params if the set goes well

    if os.environ['HOSTNAME'] == 'garteri':
        cluster_name = 'garter'
        host_nums = [5,6,7,8]
    elif os.environ['HOSTNAME'] == 'slagi':
        cluster_name = 'slag'
        host_nums = [3,4]

    host_nums = [5,6,7,8]
    expected_host_to_param_value = {cluster_name + str(n): param_to_values
                                    for n in host_nums}

    # set each parameter for all the hosts
    for param_name, value in param_to_values.items():
        set_zfs_param(param_name, value)

    # now get the params
    actaul_host_to_param_value = capture_zfs_params(param_to_values.keys())

    # and compare expected to actual
    if expected_host_to_param_value != actual_host_to_param_value:
        print('ERROR: failed to set zfs parameters', file=sys.stderr)
        sys.exit(1)

    # now actually set them



def get_zfs_params():
    '''get all the zfs params you care about.
    return as a list of dicts, a dict for each
    node, and each dict is parameter: value
    '''
    return capture_zfs_params(
        ['zfs_dirty_data_max',
         'zfs_dirty_data_max_percent',
         'zfs_max_recordsize']
    )


def set_jbod_mode():
    '''set the jbod mode for all the jbods
    then reset them all I think.
    '''
    pass

# def get_jbod_params():
#     '''get the current jbod params'''
#     text = subprocess.Popen(
#         []
#         check=True
#         shell=True
#         stdout=subprocess.PIPE
#     )
