import argparse
import os                                                                        
import sys                                                                       
import subprocess                                                                
import shutil                                                                    
                                                                                 
from cass_functions import (get_keyspaces, get_rpc_address)  

def parse_cmd():

    parser = argparse.ArgumentParser(description='Cassandra Schema Saver')
    parser.add_argument('-k', '-ks', '--keyspace',
                        required=False,
                        nargs='+',
                        help='Specify a keyspace'
    )
    return parser.parse_args()


def write_ring_info(save_path):
    
    # Ring info is not always needed, but is useful for restoring into a new
    # cluster where the ring tokens are vital
    with open(save_path + '/ring_info.txt', 'w') as f:
        nodetool = subprocess.Popen(['nodetool', 'ring'], stdout=f)
        nodetool.wait()


def write_schema(host, save_path, keyspace=None):

    # Writes the Cassandra schema to a .cql file and stores it in the save_path
    if keyspace:
        save_path = save_path + '/' + keyspace
        filename = keyspace + '_schema.cql'
        query = 'DESCRIBE KEYSPACE %s;' % keyspace
    else:
        filename = 'schema.cql'
        query = 'DESCRIBE SCHEMA;'

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    with open(save_path + '/' + filename, 'w') as f:
        query_process = subprocess.Popen(['echo', query], stdout=subprocess.PIPE)
        cqlsh = subprocess.Popen(('/bin/cqlsh', host),
                                  stdin=query_process.stdout, stdout=f)
        cqlsh.wait()
        query_process.stdout.close()

    return (save_path + '/' + filename)


def save_schema(keyspace_arg=None):

    host = get_rpc_address()
    save_path = sys.path[0] + '/.snapshots/schemas'
    keyspaces = get_keyspaces(host)
    if keyspace_arg:
        for ks in keyspace_arg:
            if ks not in keyspaces:
                print('ERROR: Invalid keyspace argument')
                exit(1)

    print('Saving schema . . .')
    print_save_path = write_schema(host, save_path)
    print('Saved schema as %s' % print_save_path)
    for ks in keyspaces:
        print_save_path = write_schema(host, save_path, ks)
        print('Saved keyspace schema as %s' % print_save_path)

    print('Compressing schema file')                                             
    shutil.make_archive(save_path, 'zip', save_path) 

    print('Saving ring information . . .')
    write_ring_info(sys.path[0] + '/.snapshots')


if __name__ == '__main__':
    cmds = parse_cmd()
    save_schema(cmds.keyspace)
