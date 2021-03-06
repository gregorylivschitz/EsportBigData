import os
from core.public_matches_2 import run_public_matches
from core.middle_ware import MmrProcess
import transfer_data_to_nyu


def run():
    transfer_from = os.path.join(os.getcwd(), 'public_matchestemp')
    transfer_to = '/scratch/gl758/public_matchestemp/'
    mmr_to = 0
    while(mmr_to <= 9000):
        mmr_process = MmrProcess()
        if mmr_process.get_first_mmr():
            first_mmr=mmr_process.get_first_mmr()
            mmr_from, mmr_to = first_mmr.mmr_from, first_mmr.mmr_to
        else:
            mmr_from, mmr_to = 6000, 6001
        run_public_matches(mmr_from, mmr_to)
        mmr_process.delete_all()
        mmr_process.insert_mmr(mmr_from+1, mmr_to+1)
        transfer_data_to_nyu.transfer_to(transfer_from, transfer_to, True)
if __name__ == '__main__':
    run()