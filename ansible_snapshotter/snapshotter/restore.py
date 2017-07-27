import os
import sys
import argparse
import subprocess
import shutil
import time
import zipfile

from cass_functions import get_rpc_address

def parse_cmd():

    parser = argparse.ArgumentParser(description='Local Snapshot Restorer')
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=True,
                        nargs='+',
                        help='Enter the host IPs'
    )
    parser.add_argument('-k', '-ks', '--keyspace',
                        required=False,
                        nargs='+',
                        help="Specify keyspace(s)"
    )
    parser.add_argument('-tb', '-t', '--table', '-cf', '--column_family',
                        required=False,
                        nargs='+',
                        help="Specify table(s)"
    )
    return parser.parse_args()


def clean_dir(path):

    # removes all files and directories from a directory
    for f in os.listdir(path):
        if os.path.isdir(path + '/' + f):
            shutil.rmtree(path + '/' + f)
        else:
            os.remove(path + '/' + f)


def make_dir(path):

    # creates a directory if it does not exist
    exists = False
    if not os.path.isdir(path):
        os.makedirs(path)
    else:
        exists = True
    return exists


def restore(hosts, keyspace_arg = None, table_arg = None):

    cqlsh_host = get_rpc_address()
    snapshot_path = sys.path[0] + '/.snapshots'
    temp_path = sys.path[0] + '/.temp'

    print('Unzipping snapshot file')
    if make_dir(temp_path):
        clean_dir(temp_path)

    zip_path = snapshot_path + '/' + cqlsh_host + '.zip'
    zipf = zipfile.ZipFile(zip_path, 'r')
    zipf.extractall(temp_path)
    zipf.close()

    print('Checking keyspace and table arguments . . .')
    keyspaces = os.listdir(temp_path)
    if keyspace_arg:
        for ks in keyspace_arg:
            if ks not in keyspaces:
                print('ERROR: Keyspace arg not in snapshot file')
                exit(1)
            if table_arg:
                for tb in table_arg:
                    if tb not in os.listdir(temp_path + '/' + ks):
                        print('ERROR: Table arg not in snapshot file')
                        exit(1)
                else:
                    tables = table_arg
        else:
            keyspaces = keyspace_arg

    print('Loading snapshot data . . .')
    for ks in keyspaces:
        if not table_arg:
            tables = os.listdir(temp_path + '/' + ks)
        print('Loading keyspace: %s' % ks)
        for tb in tables:
            print('\tLoading table: %s' % tb)
            tb_dir = temp_path + '/' + ks + '/' + tb
            subprocess.call(['/bin/sstableloader', '-d', ','.join(hosts), tb_dir])

    print('Restoration complete')


if __name__ == '__main__':

    cmds = parse_cmd()

    start = time.time()
    restore(cmds.nodes, cmds.keyspace, cmds.table)
    end = time.time()

    print('Elapsed time: %s' % (end - start))

