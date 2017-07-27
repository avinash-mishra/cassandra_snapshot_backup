## Synopsis
Cassandra does not provide an easy way to snapshot files and store these snapshots in local directories. This script does that for the user, and restores them using the same snapshot files created by the snapshotter.

This collection of scripts requires Ansible which requires ssh access keys to all
remote hosts.

For more information on Ansible: http://docs.ansible.com/ansible/intro_getting_started.html

The Ansible host needs boto3 for AWS S3 services, and the nodes need PyYaml

## Installation
Installing on the Ansible host. The only dependency currently is boto3.
```bash
sudo python setup.py install
```
or
```bash
pip install boto3
```

Installing on the nodes
```bash
cd ansible_snapshotter/
python install.py -n [nodes] # or the host group from the Ansible inventory
```

## Usage
snapshotter.py
``` bash
python snapshotter.py -d/--path          # save path
                      -n/--nodes/--hosts # host group from Ansible or host ip-addresses
                      -ks/--keyspace     # specify a keyspace (optional)
                      -tb/--table        # specify a table (optional)
                      -t/--title/--tag   # name the snapshot
                      --s3               # store in AWS S3 with the config.ini settings (flag)
                      --reload           # reinstall the scripts on the nodes (flag)
```

restore.py
``` bash
python restore.py     -d/--path          # snapshot zip file
                      -n/--nodes/--hosts # host group from Ansible or host ip-addresses; can also use config.ini
                      -ks/--keyspace     # specify a keyspace (optional)
                      -tb/--table        # specify a table (optional)
                      --s3               # retrieve from S3 with the config.ini settings; can specify key (arg) or search (flag)
                      --reload           # reinstall the scripts on the nodes (flag)
```
config.ini
``` bash
[s3-aws-info]
bucket =    # mybucket
region =    # us-west-1
account =   # account id
password =  # aws secret key

[cassandra-info]
hosts =     # name of group in ansible inventory or space/comma separated IPs
```

## How it works
snapshot.py does the following:

1. Saves the schema on one node and fetches it using Ansible

2. Takes a snapshot by calling “nodetool snapshot” and stores them in a zip file on each node

3. Fetches all zipped snapshots and stores them locally on the Ansible host

4. Uploads snapshots to AWS S3 (--s3 option)


restore.py does the following:

1. Destroys the schema if one exists on the Cassandra cluster

2. Cleans the old data files such as backup files or snapshot files on every node through Ansible

3. Copies the snapshot’s schema to a node and restores it to the database

4. Copies the snapshot SSTables to every node and loads them using Cassandra’s SSTableLoader utility


