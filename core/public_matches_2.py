import requests
import json
import time
import os
import logging
logging.basicConfig(filename='esports_runner.log',level=logging.DEBUG)

def run_public_matches(from_mmr, to_mmr):
    public_matches = requests.get("https://api.opendota.com/api/explorer?sql=select%20match_id%2C%20avg_mmr%20from%20public_matches%20%0Awhere%20avg_mmr%20%3E%3D" + str(from_mmr) + "%20and%20avg_mmr%20%3C%20" + str(to_mmr) + "%0AORDER%20BY%20avg_mmr%20desc%0A").json()
    logging.info(public_matches)
    file_path  = os.path.join(os.getcwd(), 'public_matchestemp', "from" + str(from_mmr) + "to" + str(to_mmr))

    directory = os.path.dirname(file_path)

    if not os.path.exists(directory):
        os.makedirs(directory)
    count_calls = 0
    with open(file_path, 'a') as fd:
        for public_match in public_matches['rows']:
            time.sleep(1)
            match = requests.get("https://api.opendota.com/api/matches/" + str(public_match['match_id'])).json()
            count_calls = count_calls + 1
            json.dump(match, fd)
            if count_calls == 60:
                time.sleep(62)
                count_calls = 0
    logging.info("mmr_start: {} , mmr_end: {}".format(from_mmr, to_mmr))