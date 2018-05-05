import json
import logging
import os
import requests
import time

logging.basicConfig(filename='opendota_downloader.log',level=logging.INFO)
pro_matches_data_folder = os.path.join('.', 'data', 'pro_matches_data')
pro_matches_id_folder = os.path.join('.', 'data', 'pro_matches_ids')
pro_team_data_folder = os.path.join('.', 'data', 'pro_team_data')
pro_team_players_data_folder = os.path.join('.', 'data', 'pro_team_players_data')

def update_team_players_data():
    url = 'https://api.opendota.com/api/teams'
    file_name = 'pro_team_data.json'
    new_team_data = __download_data(url)
    old_team_data = []
    
    try:
        with open(os.path.join(pro_team_data_folder, file_name), 'r') as file:
            old_team_data = json.load(file)
        
    except FileNotFoundError:
        pass
    
    with open(os.path.join(pro_team_data_folder, file_name), 'w') as file:
        json.dump(new_team_data, file)

    old_team_ids = [each_team['team_id'] for each_team in old_team_data]
    new_team_ids = [each_team['team_id'] for each_team in new_team_data]
    diff_team_ids = list(set(new_team_ids) - set(old_team_ids))

    # existing_team_data = [int(each [:-5]) for each in os.listdir(pro_team_players_data_folder)]
    # diff_team_ids = list(set(diff_team_ids) - set(existing_team_data))

    for index, each_team_id in enumerate(diff_team_ids):
        url = 'https://api.opendota.com/api/teams/{}/players'.format(each_team_id)
        file_name = '{}.json'.format(each_team_id)
        all_team_members = __download_data(api_url=url)
        only_active_members = [member for member in all_team_members if member['is_current_team_member']]
        top_5_members = sorted(only_active_members, key=lambda x: x['games_played'], reverse=True)[:5]

        with open(os.path.join(pro_team_players_data_folder, file_name), 'w') as file:
            json.dump(top_5_members, file)

def update_pro_matches_data():
    url = 'https://api.opendota.com/api/matches'

    downloaded_match_data_ids = [int(each_file[:-5]) for each_file in os.listdir(pro_matches_data_folder)]
    downloaded_match_ids = []
    for each_file in os.listdir(pro_matches_id_folder):
        with open(os.path.join(pro_matches_id_folder, each_file), 'r') as file:
            json_data = json.load(file)
            for each_match in json_data:
                downloaded_match_ids.append(each_match['match_id'])
    
    missing_matches_ids = list(set(downloaded_match_ids) - set(downloaded_match_data_ids))

    for each_match_id in missing_matches_ids:
        match_data = __download_data(url, args='/{}'.format(each_match_id))
        file_name = '{}.json'.format(each_match_id)

        with open(os.path.join(pro_matches_data_folder, file_name), 'w') as file:
            json.dump(match_data, file)


def update_pro_matches_id():
    downloaded_match_ids = []
    url = 'https://api.opendota.com/api/proMatches'

    for each_file in os.listdir(pro_matches_id_folder):
        with open(os.path.join(pro_matches_id_folder, each_file), 'r') as file:
            json_data = json.load(file)
            for each_match in json_data:
                downloaded_match_ids.append(each_match['match_id'])

    new_matches = []
    new_matches += __download_data(api_url=url)
    new_match_ids = [each_new_match['match_id'] for each_new_match in new_matches]

    while min(new_match_ids) > max(downloaded_match_ids):

        new_matches += __download_data(api_url=url, args='/?less_than_match_id={}'.format(min(new_match_ids)))
        new_match_ids = [each_new_match['match_id'] for each_new_match in new_matches]        

    only_new_matches = [each_match for each_match in new_matches if each_match['match_id'] not in downloaded_match_ids]
    
    if only_new_matches:
        file_name = 'pro_matches_{}.json'.format(len(os.listdir(pro_matches_id_folder)))
        with open(os.path.join(pro_matches_id_folder, file_name), 'w') as file:
            json.dump(only_new_matches, file)


def __download_data(api_url, args=None):
    __api_url = api_url
    
    if args:
        __api_url += args 

    wait_multiplier = 1
    
    while 1:
        time.sleep(2)
        r = requests.get(__api_url)
        
        if r.status_code == 502 or r.status_code == 429:
            logging.info('{} encounterd. waiting for {} seconds'.format(r.status_code, 30 * wait_multiplier))
            time.sleep(30 * wait_multiplier)
            wait_multiplier *= 2
            continue

        elif r.status_code == 200 and r.json():
            return r.json()


if __name__ == '__main__':
    update_pro_matches_id()
    update_pro_matches_data()
    # update_team_players_data()
