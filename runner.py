import os

from core.scraper.pinnacle_scraper import PinnacleScrapper
from core.transfers import TransferToNYUCluster

def run():
    pinnacle_scraper = PinnacleScrapper('dota-2', 'https://www.pinnacle.com')
    transfer_nyu = TransferToNYUCluster()
    all_files = os.listdir(os.path.join(os.getcwd(), 'data'))
    for game_gambling_file in all_files:
        transfer_from = os.path.join(os.getcwd(), 'data', '{}'.format(game_gambling_file))
        transfer_to = '/scratch/gl758/data_gambling/{}'.format(game_gambling_file)
        transfer_nyu.transfer(transfer_from, transfer_to)

if __name__ == '__main__':
    run()