import os
from core.public_matches import run_public_matches
from core.scraper.pinnacle_scraper import PinnacleScrapper
from core.transfers import TransferToNYUCluster
import transfer_data_to_nyu

def run():
    pinnacle_scraper = PinnacleScrapper('dota-2', 'https://www.pinnacle.com')
    pinnacle_scraper.scrap()
    transfer_from = os.path.join(os.getcwd(), 'gamble_data')
    transfer_to = '/scratch/gl758/data_gambling/'
    transfer_data_to_nyu.transfer_to(transfer_from, transfer_to, True)

if __name__ == '__main__':
    run()