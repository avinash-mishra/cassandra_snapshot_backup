import os
import sys
import argparse
import subprocess
import shutil
import datetime
import time

from cass_functions import (get_data_dir, get_keyspaces, get_dir_structure,
                            cassandra_query)

# nodetool only works with localhost, cqlsh only works with the node's ip

def parse_cmd():

    parser = argparse.ArgumentParser(description='Snapshotter')

    parser.add_argument('-d', '--path',
                        type=check_dir,
                        required=True,
                        help='Specify path to save snapshots'
    )
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
    parser.add_argument('-t', '--title', '--tag', '--name',
                        required=False,
                        help='Enter title/name for snapshot'
    )
    parser.add_argument('-n', '--node', '--host',
                        required=False,
                        help='Enter the host ip'
    )
    return parser.parse_args()


def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')

    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def write_schema(host, save_path, keyspace = None):

    if keyspace:
        save_path = save_path + '/' + keyspace + '/'
        filename = keyspace + '_schema.cql'
        query = ("DESCRIBE KEYSPACE %s;" % keyspace)
    else:
        save_path = save_path + '/'
        filename = 'schema.cql'
        query = ("DESCRIBE SCHEMA;")

    with open(save_path + '/' + filename, 'w') as f:
        query_process = subprocess.Popen(['echo', query], stdout=subprocess.PIPE)
        cqlsh = subprocess.Popen(('/bin/cqlsh', host),
                                  stdin=query_process.stdout, stdout=f)
        query_process.stdout.close()

    return (save_path + filename)


def run_snapshot(title, keyspace=None, table=None):

    cmd = 'nodetool snapshot -t %s ' % title
    if keyspace:

        if table:
            cmd = '%(cmd)s -cf %(table)s ' % dict(cmd=cmd, table=table)
        cmd = cmd + keyspace

    subprocess.call(cmd.split())


def snapshot(host, save_path, title_arg=None, keyspace_arg=None, table_arg=None):
    # nodetool can only run localhost and cqlsh can only run on host argument
    # clear snapshot in default snapshot directory
    print('Checking Cassandra status . . .')
    try:
        subprocess.check_output(['nodetool', 'status'])
    except:
        raise Exception('Cassandra has not yet started')

    # TODO hacky
    # get_keyspaces() calls cassandra_query which checks if the host works
    keyspaces = get_keyspaces(host) # set of keyspaces
    if len(keyspaces) == 0: # edge case
        raise Exception('No keyspaces to snapshot. If Connection Error, ' +
              'host option is invalid.')

    if not title_arg:
        title = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    else:
        title = title_arg

    save_path = save_path + title
    if os.path.exists(save_path):
        raise Exception('Error: Snapshot directory already created')

    print('Checking keyspace arguments . . .')
    if keyspace_arg: # checks if keyspace argument exists in database
        for ks in keyspace_arg:
            if ks not in keyspaces:
                raise Exception('Keyspace "%s" not found.' % ks)
        else:
            keyspaces = set(keyspace_arg)
    else:
        print('No keyspace arguments.')

    structure = get_dir_structure(host, keyspaces) # basic schema in json format
    print('Checking table arguments . . .')
    if table_arg:
        if not keyspace_arg or len(keyspace_arg) != 1:
            raise Exception('Only one keyspace can be specified with table arg')
        ks = next(iter(keyspaces)) # retrieve only element in set
        for tb in table_arg:
            if tb not in structure[ks]:
                raise Exception('Table "%s" not found in keyspace "%s"' % (tb, ks))
        else:
            tables = set(table_arg)
    else:
        print('No table arguments.')
    print('Valid arguments.\n')

    print('Clearing previous cassandra data snapshots . . .')
    subprocess.call(['nodetool', 'clearsnapshot'])

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

    print('Saving schema . . .')
    print_save_path = write_schema(host, save_path)
    print('Saved schema as %s' % print_save_path)
    for ks in keyspaces:
        print_save_path = write_schema(host, save_path, ks)
        print('Saved keyspace schema as %s' % print_save_path)
        pass

    print('\nProcess complete. Snapshot stored in %s\n' % save_path)


if __name__ == '__main__':
    cmds = parse_cmd()

    if cmds.path.endswith('\\') or cmds.path.endswith('/'):
        save_path = cmds.path
    else:
        save_path = cmds.path + '/'
    
    if cmds.node:
        host = cmds.node
    else:
        host = 'localhost'

    start = time.time()    
    snapshot(host, save_path, cmds.title, cmds.keyspace, cmds.table)
    end = time.time()

    print('Elapsed time: %s' % (end - start))
