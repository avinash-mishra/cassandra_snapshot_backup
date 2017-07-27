import os
import sys
import shutil
import argparse

from cass_functions import (cassandra_query, get_data_dir, get_keyspaces,
                            get_table_directories, get_dir_structure,
                            _SYSTEM_KEYSPACES)

def parse_cmd():

    parser = argparse.ArgumentParser(description='Cleaner')

    parser.add_argument('-n', '--node', '--host',
                        required=False,
                        help='Specify local host/node ip'
    )
    return parser.parse_args()


def data_cleaner(host='localhost', backups=False):
    # This fuction finds inactive data directories and removes them
    # This includes unused keyspace directories and table directories

    keyspaces = get_keyspaces(host, system=True)
    if len(keyspaces) == 0: # there should always be system keyspaces
        raise Exception('Invalid host parameter')
    structure = get_dir_structure(host, keyspaces)
    cass_data_dir = get_data_dir()

    print('Deleting old keyspaces . . .')
    for ks in os.listdir(cass_data_dir):
        if ks not in keyspaces:
            print('\tDeleting: ' + cass_data_dir + '/' + ks)
            shutil.rmtree(cass_data_dir + '/' + ks)

    print('\nDeleting old tables . . .')
    for keyspace in keyspaces:
        if keyspace not in _SYSTEM_KEYSPACES:
            print('\nProcessing keyspace: %s' % keyspace)
            # should only be directories in this folder
            data_dirs = set(os.listdir(cass_data_dir + '/' + keyspace))
            table_dirs = set()

            for table in structure[keyspace].keys():
                table_dirs.add(structure[keyspace][table])

            inactive_dirs = data_dirs - table_dirs

            print('Removing inactive directories . . .')
            for d in inactive_dirs:
                print('\tDeleting: ' + cass_data_dir + '/' + keyspace + '/' + d)
                shutil.rmtree(cass_data_dir + '/' + keyspace + '/' + d)

            if backups:
                print('Removing old backup db files')
                for d in table_dirs:
                    clean_directory(cass_data_dir + '/' + keyspace + '/' + d + '/backups')


def clean_directory(table_directory):                                            
    # TODO does incremental backups work with this?                              
    for f in os.listdir(table_directory):                                        
        if f.endswith('.db') or f.endswith('.crc32') or f.endswith('.txt'):                              
            os.remove(table_directory + '/' + f)                                 
                                                

if __name__ == '__main__':

    cmds = parse_cmd()

    if cmds.node:
        host = cmds.node
    else:
        host = 'localhost'

    data_cleaner(host) #TODO backups option?


