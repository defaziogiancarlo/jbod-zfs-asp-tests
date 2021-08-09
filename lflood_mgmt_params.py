'''Set Params of the nodes for zfs and for jbod.
outputs a timestamped file that has the parameters.
This is useful for comparing with the ior/mdtest stuff
that you will do on catalyst. It might resolve some confusions
chronologically.
'''

import argparse
import datetime
import os
import re
import subprocess
import sys
import yaml

#if os.environ['HOSTNAME'] == 'garteri':
lustre_nodes = 'egarter[5-8]'
#elif os.environ['HOSTNAME'] == 'slagi':
#lustre_nodes = 'eslag[3-4]'

zfs_params_command = (
    'pdsh -w egarter[1-8] \"echo -n \'{} \' && '
    'cat /sys/module/zfs/parameters/{}\"'
)

# TODO, make this work for list, set and dict
# for dict, the value would have to be a dict
# for creating new dict (d is None) you can specify the value
# type, which can be list, set, dict
# for (d is not None) value_type must match the type of the values
# or maybe make utility functions that convery from list
# values to set values, and from lists of 2-tuples to dicts
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


def combine_key_to_dicts(key_to_dicts):
    '''Given a list of dicts, where the values of the dicts are themselves
    dicts, combine all everything into one dict. Assumes
    all the dicts have the same keys (the outermost keys)
    '''
    combined = {}
    keys = list(key_to_dicts[0].keys())
    for key in keys:
        combined[key] = {}
        for x in key_to_dicts:
            combined[key].update(x[key])
    return combined


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

def capture_zfs_params2(param_list):
    '''For each param in the list, get the param,
    and combine everthign into a single dict of form
    {host: {}}
    '''
    capture_params = [capture_zfs_param(param) for param in param_list]
    # now combine the dicts

    return combine_key_to_dicts(capture_params)

def set_zfs_param(param_name, value):
    '''For all the hosts, set the zfs param to the value.'''
    cmd = (
        f'pdsh -w {lustre_nodes} '
        f'\'echo {value} > /sys/module/zfs/parameters/{param_name}\''
    )
    subprocess.run(
        [cmd],
        check=True,
        shell=True,
    )


def get_recordsize():
    '''get zfs record size of the OSTs'''
    text = subprocess.run(
        ['pdsh -w egarter[5,6,7,8] zfs get -H recordsize'],
        check=True,
        shell=True,
        stdout=subprocess.PIPE,
    ).stdout.decode().split('\n')

    # now find the line that are for OSTs
    ost_lines = [line for line in text if 'ost' in line.lower()]

    recordsizes = {}
    # now grab the node name and the size
    for line in ost_lines:
        tokens = line.split()
        node, _ = tokens[1].split('/')
        recordsize = tokens[3]
        recordsizes[node] = {'recordsize': recordsize}

    return recordsizes


def set_zfs_params(param_to_values):
    '''Set all the nodes to have the given param values.'''
    # create a check dict, as in the dict you expect to get from
    # capture_zfs_params if the set goes well

    # if os.environ['HOSTNAME'] == 'garteri':
    cluster_name = 'garter'
    #     host_nums = [5,6,7,8]
    # elif os.environ['HOSTNAME'] == 'slagi':
    #cluster_name = 'slag'
    #host_nums = [3,4]

    host_nums = [5,6,7,8]
    expected_host_to_param_value = {cluster_name + str(n): param_to_values
                                    for n in host_nums}

    # set each parameter for all the hosts
    for param_name, value in param_to_values.items():
        set_zfs_param(param_name, value)

    # now get the params
    actual_host_to_param_value = capture_zfs_params(param_to_values.keys())

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


slag_params_orig = {
    'zfs_dirty_data_max': '4294967296',
    #'zfs_dirty_data_max_percent': '10',
    #'zfs_max_recordsize': '1048576'
}

slag_params_new = {
    'zfs_dirty_data_max': '8589934592',
    #'zfs_dirty_data_max_percent': '20',
    #'zfs_max_recordsize': '1048576'
}

slag_p = {'zfs_dirty_data_max': '8589934592'}

# def get_jbod_params():
#     '''get the current jbod params'''
#     text = subprocess.Popen(
#         []
#         check=True
#         shell=True
#         stdout=subprocess.PIPE
#     )


def get_jbod_names():
    '''Get the names of the available jbods.
    The jbods are listed in /etc/hosts.
    There is an ip address, then 2 naming schemes.
    The third word uses the (a,b) naming scheme and
    that is the name I capture.
    '''

    text = subprocess.run(
        ['awk', '/jbod/ {print $3}', '/etc/hosts'],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.decode().split('\n')

    jbod_names = [n.strip() for n in text if n]

    return jbod_names

def get_jbod_mode(jbod_name):
    '''Ask all the jbods for their mode.
    Will return None if it can't find the zone number
    in the text.
    '''
    text = subprocess.run(
        ['/g/g0/defazio1/supermicro_system_util.sh',
         'zone',
         '-p', 'ADMIN',
         '-u', 'ADMIN',
         '-h', jbod_name,
         '-a', 'get',],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.decode().split('\n')

    # look for the line with ZONE in it
    # and grab the last teken
    for line in text:
        if line.startswith('ZONE'):
            return line.split()[-1]


def get_all_jbod_modes():
    '''return a dict of jbod_name to mode.'''
    jbod_names = get_jbod_names()
    return {
        name: get_jbod_mode(name)
        for name in jbod_names
        if name.endswith('a')
    }

def set_jbod_mode(jbod_name, mode):
    '''set the jbod mode, then check that it was set.'''

    mode = str(mode)
    text = subprocess.run(
        ['/g/g0/defazio1/supermicro_system_util.sh',
         'zone',
         '-p', 'ADMIN',
         '-u', 'ADMIN',
         '-h', jbod_name,
         '-a', 'set',
         '-m', mode],
        check=True,
    )
    # now check to make sure it worked
    actual_mode = get_jbod_mode(jbod_name)

    if actual_mode != mode:
        print('failed to set {jbod_name} to mode {mode}', file=sys.stderr)
        exit(1)

# you might need to only do 1 of each
# for example for jbod5a and jbod5b, I think
# setting one will set the other
def set_and_reset_jbods():
    pass

def set_all_jbod_mode(mode):
    '''Set all the jbods to the given mode.'''
    jbod_names = get_jbod_names()
    for name in jbod_names:
        if name.ednswith('a'):
            set_jbod_mode(name, mode)

def get_params():
    '''Get all the params for zfs and jbod, and
    dump them to a timestamped file.
    '''
    params = {}
    # get jbod modes
    jbod_modes = get_all_jbod_modes()
    params['jbod_to_mode'] = jbod_modes

    # TODO get recordsize
    # get zfs parameters
    zfs_params = get_zfs_params()
    params['zfs_params'] = zfs_params

    return params

def dump_params(params):
    '''dump the params to a timestamped file.'''

    params_dir = pathlib.Path('/g/g0/defazio1/garter_jbod_params')
    time_now = str(datetime.datetime.now()).replace(' ', '_').replace(':', '')
    params_path = params_dir / time_now
    with open(params_path, 'w') as f:
        yaml.safe_dump(params, f)


def all_the_same(items):
    '''Check that every item in a list is identical.
    Or, for a dict, check the the values are all the same.
    '''
    if isinstance(items, dict):
        items = list(items.values())
    if len(items) == 0 or len(items) == 1:
        return True
    reference = items[0]
    for x in items[1:]:
        if x != reference:
            return False
    return True


def zfs_check_set_and_check(params):
    '''For zfs, check the current params,
    set to the given params,
    then check that the params are set.
    '''
    current_params = get_zfs_params()
    params_no_keys = list(current_params.values())
    if not all_the_same(params_no_keys):
        print('ERROR: zfs params do not match on all nodes', file=sys.stderr)
        sys.exit(1)
    print('initial params:')
    print(params_no_keys[0])

    # actually set the params
    set_zfs_params(params)

    # now check that the params got set correctly
    current_params = get_zfs_params()
    params_no_keys = list(current_params.values())
    if not all_the_same(params_no_keys):
        print(
            'ERROR: zfs params do not match on all nodes after being set',
            file=sys.stderr
        )
        sys.exit(1)
    #print('initial params:')
    if params != params_no_keys[0]:
        print(
            'ERROR: zfs params were not set correctly',
            file=sys.stderr
        )
        sys.exit(1)

    print('zfs params set to:')
    print(params_no_keys[0])

default_zfs_params = {
    'zfs_dirty_data_max': '4294967296',
    'zfs_dirty_data_max_percent': '10',
    'zfs_max_recordsize': '1048576'
}

def get_and_display_params():
    '''This is just the default of what you want,
    which is to get the setting for the zfs and jbods,
    then print them out in a form that you can copy and
    paste into you test-run to record the params.
    '''
    # get the zfs params
    current_params = get_zfs_params()
    #print(current_params)
    # get record size
    record_sizes = get_recordsize()
    #print(record_sizes)
    # get jbod mode
    jbod_modes = get_all_jbod_modes()
    #print(jbod_modes)
    # check that all nodes match
    # put into one big dict and print it
    if not (all_the_same(current_params) and
        all_the_same(record_sizes) and
        all_the_same(jbod_modes)
    ):
        print('ERROR: params not all the same')
        sys.exit(1)

    all_params = list(current_params.values())[0]
    all_params.update(list(record_sizes.values())[0])
    jbod_mode = list(jbod_modes.values())[0]
    all_params.update({'jbod_mode': jbod_mode})
    print(all_params)


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--params',
        action='store_true',
        help=(
            'list the zfs and jbod_params, '
            'will complain if not all params match'
        )
    )
    return parser

def main():
    parser = make_parser()
    args = vars(parser.parse_args())
    if args['params']:
        get_and_display_params()

if __name__ == '__main__':
    main()
