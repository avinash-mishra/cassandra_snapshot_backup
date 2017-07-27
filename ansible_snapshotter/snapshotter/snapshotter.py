import os
import sys
import argparse
import subprocess
import shutil
import time

from cass_functions import (get_data_dir, get_keyspaces, get_dir_structure,
                            get_rpc_address, cassandra_query, check_host)

def parse_cmd():

    parser = argparse.ArgumentParser(description='Local Snapshotter')
    parser.add_argument('-k', '-ks', '--keyspace',
                        required=False,
                        nargs='+',
                        help='Specify a keyspace'
    )
    parser.add_argument('-tb', '--table', '-cf', '--column-family',
                        required=False,
                        nargs='+',
                        help='Enter table(s) corresponding to a single keyspace'
    )
    return parser.parse_args()


def run_snapshot(title, keyspace=None, table=None):

    cmd = 'nodetool snapshot -t %s ' % title
    if keyspace:

        if table:
            cmd = '%(cmd)s -cf %(table)s ' % dict(cmd=cmd, table=table)
        cmd = cmd + keyspace

    subprocess.call(cmd.split())


def snapshot(keyspace_arg=None, table_arg=None):

    # nodetool can only run localhost and cqlsh can only run on host argument
    host = get_rpc_address()
    title = host # all local snapshots are named by its ip address or rpc_address
    save_root = sys.path[0] + '/.snapshots/'

    if check_host(host) != 0:
        print('ERROR: Invalid host, check rpc_address in this node\'s yaml file')
        exit(1)
    keyspaces = get_keyspaces(host) # set; retrieves through cqlsh
    if len(keyspaces) == 0: # edge case
        print('ERROR: No keyspaces found')
        exit(1)

    print('Checking keyspace arguments . . .')
    if keyspace_arg:
        for ks in keyspace_arg:
            if ks not in keyspaces:
                print('ERROR: Keyspaces "%s" not found.' % ks)
                exit(1)
        else:
            keyspaces = set(keyspace_arg)
    else:
        print('No keyspace arguments.')

    structure = get_dir_structure(host, keyspaces) # basic schema in json format
    print('Checking table arguments . . .')
    if table_arg:
        if not keyspace_arg or len(keyspace_arg) != 1:
            print('ERROR: Only one keyspace can be specified with table arg')
            exit(1)
        ks = next(iter(keyspaces)) # retrieve only element in set
        for tb in table_arg:
            if tb not in structure[ks]:
                print('ERROR: Table "%s" not found in keyspace "%s"' % (tb, ks))
                exit(1)
        else:
            tables = set(table_arg)
    else:
        print('No table arguments.')
    print('Valid arguments.\n')

    print('Clearing previous cassandra data snapshots . . .')
    subprocess.call(['nodetool', 'clearsnapshot'])
    if os.path.isdir(save_root): # remove old snapshots from .snapshot
        for f in os.listdir(save_root):
            if os.path.isdir(save_root + f):
                shutil.rmtree(save_root + f)
            else:
                os.remove(save_root + f)

    save_path = save_root + title
    if os.path.exists(save_path):
        print('ERROR: Snapshot save path conflict')
        exit(1)

    print('Saving snapshot into %s . . .' % save_path)
    print('Producing snapshots . . .')
    if keyspace_arg:
        if table_arg:
            ks = next(iter(keyspaces))
            for table in tables:
                run_snapshot(title, ks, table)
        else:
            run_snapshot(title, ' '.join(keyspaces))
    else:
        run_snapshot(title)

    cassandra_data_dir = get_data_dir()
    for ks in keyspaces:
        if not table_arg:
            tables = structure[ks]
        for tb in tables:
            save_table_path = '%(save_path)s/%(keyspace)s/%(table)s/' \
                          % dict(save_path = save_path,
                                 keyspace  = ks,
                                 table     = tb)
            load_dir = '%(data_dir)s/%(keyspace)s/%(table_dir)s/snapshots/%(ss_title)s' \
                   % dict(data_dir  = cassandra_data_dir,
                          keyspace  = ks,
                          table_dir = structure[ks][tb],
                          ss_title  = title)
            print('Storing %s in %s' % (tb, save_table_path))
            shutil.copytree(load_dir, save_table_path)

    print('Compressing snapshot file')
    shutil.make_archive(save_path, 'zip', save_path)

    print('\nProcess complete. Snapshot stored in %s\n' % save_path)


if __name__ == '__main__':
    cmds = parse_cmd()

    start = time.time()
    snapshot(cmds.keyspace, cmds.table)
    end = time.time()

    print('Elapsed time: %s' % (end - start))
