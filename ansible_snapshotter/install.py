import argparse
import re
try:
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser

from utils import run_playbook

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Script Installer')
    parser.add_argument('-n', '--nodes', '--hosts',
                        nargs='+',
                        required=False,
                        help='Specify the hosts from the Ansible inventory or ' +
                             'through a space separated list'
    )
    return parser.parse_args()
    

def install(nodes):

    playbook_args = {
        'nodes' : ' '.join(nodes),
    }

    return_code = run_playbook('install.yml', playbook_args)
    if return_code != 0:
        print('Error running ansible script')
    else:
        print('Installation complete')


if __name__ == '__main__':
    cmds = parse_cmd()

    if not cmds.nodes:
        config = ConfigParser()
        if len(config.read('config.ini')) == 0:
            raise Exception('ERROR: Cannot find config.ini in script directory')
        nodes = re.findall('[^,\s\[\]]+', config.get('cassandra-info', 'hosts'))
        if not nodes:
            raise Exception('Hosts argument in config.ini not specified')
    else:
        nodes = cmds.nodes

    install(nodes)
