import argparse
import os
import subprocess
import shutil
import time

from cass_functions import check_host, get_rpc_address, get_yaml_var

_TIMEOUT = 120 # seconds
_SLEEP = 2 


def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Resetter')
    parser.add_argument('-s', '--stage',
                        required=False,
                        help='Specify which stage to run'
    )
    return parser.parse_args()


def shutdown():
    
    print('Stopping daemon')
    stopdaemon = subprocess.Popen(('nodetool', 'stopdaemon'))
    stopdaemon.wait()

    print('Stopping service')
    stopservice = subprocess.Popen(('sudo', 'service', 'cassandra', 'stop'))
    stopservice.wait()

    print('Removing logs, commitlogs, caches, and data')
    remove_dirs = [
        get_yaml_var('commitlog_directory'),
        get_yaml_var('saved_caches_directory'),
        get_yaml_var('data_file_directories')[0],
        '/var/log/cassandra'
    ]
    for d in remove_dirs:
        if os.path.isdir(d):
            shutil.rmtree(d)


def start():

    print('Starting service')
    startservice = subprocess.Popen(('sudo', 'service', 'cassandra', 'start'))
    startservice.wait()

    # process will not continue without calling script with nohup
    print('Starting Cassandra') 
    start_cassandra = subprocess.Popen(('/usr/sbin/cassandra'), shell=True)

    start = time.time()
    current = start
    host = get_rpc_address()
    while check_host(host) != 0:
        print('Time elapsed waiting for Cassandra: %s' % (current - start))
        current = time.time()
        if current - start > _TIMEOUT:
            print('ERROR: Timed out waiting for cassandra to start.' +
                  'Try increasing _TIMEOUT value.')
            exit(1)
        time.sleep(_SLEEP)

    print('Hard reset complete')


if __name__ == '__main__':

    cmds = parse_cmd()
    
    start_time = time.time()
    if cmds.stage:

        if cmds.stage == 'shutdown':
            shutdown()
        elif cmds.stage == 'start':
            start()

    else:
        shutdown()
        start()
    print('Process took %s seconds to complete' % (time.time() - start_time))
