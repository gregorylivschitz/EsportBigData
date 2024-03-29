import os
from core.transfers import TransferToNYUCluster
import logging
logging.basicConfig(filename='esports_runner.log',level=logging.INFO)


def transfer_to(transfer_from, transfer_to, need_clean_up):
    logging.info("starting transfer from {} to {}".format(transfer_from, transfer_to))
    transfer_nyu = TransferToNYUCluster()
    if not os.path.exists(transfer_from):
        os.makedirs(transfer_from)
    data_files = os.listdir(transfer_from)

    for data_file in data_files:
        transfer_from_file = os.path.join(transfer_from, '{}'.format(data_file))
        transfer_nyu.transfer(transfer_from_file, transfer_to)
    if need_clean_up:
        clean_up(transfer_from)

def clean_up(transfer_from):
    logging.info("cleaning up {}".format(transfer_from))
    files = os.listdir(transfer_from)
    for file in files:
        os.remove(os.path.join(transfer_from, file))