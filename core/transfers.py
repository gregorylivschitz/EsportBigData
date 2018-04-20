import json

import paramiko as paramiko
from sshtunnel import SSHTunnelForwarder
from scp import SCPClient

class TransferToNYUCluster():

    def __init__(self):
        with open('config.json', 'r') as f:
            self.config = json.load(f)

    def transfer(self, transfer_from_path, transfer_to_path):
        server = self.create_tunnel()
        server.start()
        print(server.local_bind_port)
        ssh_dumbo = self.createSSHClient(self.config['dumbo_cluster']['server'],
                                         server.local_bind_port,
                                   self.config['dumbo_cluster']['username'],
                                   self.config['dumbo_cluster']['password'])
        transport = ssh_dumbo.get_transport()
        scp_dumbo = SCPClient(transport)
        scp_dumbo.put(transfer_from_path, transfer_to_path)
        scp_dumbo.close()
        server.stop()

    def createSSHClient(self, server, port, user, password):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(server, port, user, password)
        return client

    def create_tunnel(self):
        server = SSHTunnelForwarder(
            self.config['nyu_cluster']['server'],
            ssh_username=self.config['nyu_cluster']['username'],
            ssh_password=self.config['nyu_cluster']['password'],
            remote_bind_address=('localhost', 22)
        )
        return server