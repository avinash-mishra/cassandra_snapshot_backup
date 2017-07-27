from cass_functions import cassandra_query, get_keyspaces, get_rpc_address

def destroy_schema(host):

    keyspaces = get_keyspaces(host)

    for k in keyspaces:
        print('Dropping keyspace: %s' % k)
        cassandra_query(host, 'DROP KEYSPACE %s;' % k)

if __name__ == '__main__':
    destroy_schema(get_rpc_address())
