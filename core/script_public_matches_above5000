import requests
import json
import time
import os

from_mmr = [5000, 5500, 6000, 6500]
to_mmr = [5500, 6000, 6500, 9500]
increments = [1, 50, 100, 3000]

print(os.getcwd())

for k in range (0, 4):
    for i in range(from_mmr[k], to_mmr[k], increments[k]):
        public_matches = requests.get("https://api.opendota.com/api/explorer?sql=select%20match_id%2C%20avg_mmr%20from%20public_matches%20%0Awhere%20avg_mmr%20%3E%3D" + str(i) + "%20and%20avg_mmr%20%3C%20" + str(i + increments[k]) + "%0AORDER%20BY%20avg_mmr%20desc%0A").json()
        file_path  = os.path.join(os.getcwd(), 'public_matchestemp', "from" + str(i) + "to" + str(i + increments[k]))
        
        directory = os.path.dirname(file_path)

        if not os.path.exists(directory):
            os.makedirs(directory)      

        with open(file_path, 'a') as fd:
            for public_match in public_matches['rows']:
                time.sleep(1)
                match = requests.get("https://api.opendota.com/api/matches/" + str(public_match['match_id'])).json()
                json.dump(match, fd)
