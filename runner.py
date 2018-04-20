import os

from core.scraper.pinnacle_scraper import PinnacleScrapper
from core.transfers import TransferToNYUCluster

def run():
    pinnacle_scraper = PinnacleScrapper('dota-2', 'https://www.pinnacle.com')
    transfer_nyu = TransferToNYUCluster()
    transfer_from = os.path.join(os.getcwd(), 'data', 'dota-2-esl-one-birmingham-cis-qualifier.csv')
    transfer_to = os.path.join(os.sep, 'stratch', 'gl758', 'data_gambling')
    transfer_to = os.path.join(os.sep, 'home', 'ubuntu', 'big_data')
    transfer_nyu.transfer(transfer_from, transfer_to)

if __name__ == '__main__':
    run()