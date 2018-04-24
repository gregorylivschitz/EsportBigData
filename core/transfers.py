import json

import paramiko as paramiko
from sshtunnel import SSHTunnelForwarder
from scp import SCPClient

class TransferToNYUCluster():

    def __init__(self):
        with open('config.json', 'r') as f:
            self.config = json.load(f)

    def transfer(self, transfer_from_path, transfer_to_path):
        client = self.create_tunnel(transfer_from_path, transfer_to_path)

    def create_ssh_connection(self, client, server, port, user, password):
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(server, port, user, password)
        return client

    def create_tunnel(self, transfer_from_path, transfer_to_path):
        with SSHTunnelForwarder(
                (self.config['nyu_cluster']['server'], 22),
                ssh_username=self.config['nyu_cluster']['username'],
                ssh_password=self.config['nyu_cluster']['password'],
                remote_bind_address=("dumbo.hpc.nyu.edu", 22),
                local_bind_address=('127.0.0.1', self.config['dumbo_cluster']['port'])
        ) as tunnel:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.config['dumbo_cluster']['server'],
                           self.config['dumbo_cluster']['port'],
                                   self.config['dumbo_cluster']['username'],
                                   self.config['dumbo_cluster']['password'])
            transport = client.get_transport()
            scp_dumbo = SCPClient(transport)
            scp_dumbo.put(transfer_from_path, transfer_to_path)
            client.close()

