import yaml
import os
import subprocess
import re

_SYSTEM_KEYSPACES = set(['system_schema',
                         'system_auth',
                         'system',
                         'system_distributed',
                         'system_traces'])

_YAML_LOCATIONS = ['/etc/cassandra/conf/', # package install on centos
                   '/etc/cassandra/',      # ubuntu package install
                   '/etc/dse/cassandra/'   # datastax enterprise package
                  ] #TODO user input yaml locations, other locations

    
# Cassandra YAML related functions
def get_yaml_var(var):
    # This function uses cassandra.yaml to find a specific variable in it

    for loc in _YAML_LOCATIONS:
        if os.path.exists(loc + 'cassandra.yaml'):
            yaml_dir = loc + 'cassandra.yaml'
            break
    else:
        # TODO user inputs yaml location (too many args to pass on call?)
        raise Exception('Could not find cassandra YAML file.')

    with open(yaml_dir, 'r') as f:
        cass_yaml = yaml.load(f)
    return cass_yaml[var]
    

def get_data_dir():
    return get_yaml_var('data_file_directories')[0]


def get_rpc_address():
    return get_yaml_var('rpc_address')


# Cassandra Query Related Function
def cassandra_query(host, query, test=False):
    # This function takes in a cassandra query and returns the output

    if type(query) is str:
        query = ['echo', query]
    elif type(query) is not list:
        raise Exception('Query not recognized')

    query_process = subprocess.Popen(query, stdout=subprocess.PIPE)
    cqlsh = subprocess.Popen(('/bin/cqlsh', host),
                             stdin=query_process.stdout,
                             stdout=subprocess.PIPE)
    query_process.stdout.close()
    output = cqlsh.communicate()[0]
    query_process.wait()
    if test:
        output = cqlsh.returncode
    return output


def check_host(host):
    return cassandra_query(host, 'exit', test=True)


def get_keyspaces(host, system=False):
    # This function calls Cassandra to find the keyspaces in the database

    keyspaces_string = cassandra_query(host, 'DESCRIBE keyspaces;')
    keyspaces = set(keyspaces_string.strip().split())
    if not system:
        keyspaces = keyspaces - _SYSTEM_KEYSPACES
    return keyspaces


def get_table_directories(host, keyspace):
    # This function calls Cassandra to retrieve the tables and their
    # corresponding uuid

    cmd = ("SELECT table_name, id FROM system_schema.tables \
            WHERE keyspace_name='%s';" % keyspace)
    query = cassandra_query(host, cmd)

    # format of query is as follows, may need to be updated
    '''

    table_name  | id
    ------------+---------------------------
    first_table | first_uuid
    . . .       | . . .
    last_table  | last_uuid

    (num rows)
    '''

    # finds table and uuid; could be more efficient?
    matcher = '(\w{1,})\ \|\ ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
    r = re.compile(matcher)
    table_directory = dict(re.findall(r, query))
    for table, uuid in table_directory.iteritems(): # strip '-' from uuid
        table_directory[table] = table + '-' + uuid.replace('-', '')

    return table_directory


def get_dir_structure(host, keyspaces):
    # This function stores the basic Cassandra schema in the following format
    # where each value to each corresponding table is that table's data
    # directory

    '''
    structure: {
        keyspace1 : {
            'table1' : 'directory1',
            'table2' : 'directory2',
            . . .
        },
        keyspace2 : {
            . . .
        }
    }
    '''

    structure = {}
    for keyspace in keyspaces:
        structure[keyspace] = get_table_directories(host, keyspace)

    return structure
