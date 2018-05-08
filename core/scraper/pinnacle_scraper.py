import re
import json
import os
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from core.middle_ware import UrlVisited
import logging
logging.basicConfig(filename='esports_runner',level=logging.INFO)

class PinnacleScrapper():

    def __init__(self, game_name, base_url):
        with open('config.json', 'r') as f:
            self.config = json.load(f)
        self.game_name = game_name
        self.base_url = base_url


    def find_leagues(self):
        pinnicale_base_url = requests.get('{}/en/rtn'.format(self.base_url))
        if pinnicale_base_url.status_code == 200:
            pinnicale_html = pinnicale_base_url.content
            pinnicale_soup = BeautifulSoup(pinnicale_html, 'html.parser')
            pinnicale_links = pinnicale_soup.find_all("a", href=re.compile('/en/odds/match/e-sports/{}/'.format(
                self.game_name)))
            league_links = []
            for pinnicale_link in pinnicale_links:
                league_links.append(pinnicale_link.get('href'))
            return league_links

    def find_odds_for_league(self):
        url_visited = UrlVisited()
        league_links = self.find_leagues()
        for league_link in league_links:
            league_url = '{}{}'.format(self.base_url, league_link)
            if not url_visited.exists(url_name=league_url):
                options = webdriver.ChromeOptions()
                options.add_argument('headless')
                if 'chrome_location' in self.config:
                    browser = webdriver.Chrome(self.config['chrome_location'],chrome_options=options)
                else:
                    browser = webdriver.Chrome(chrome_options=options)
                browser.get(league_url)
                html = browser.page_source
                league_soup = BeautifulSoup(html, 'html.parser')
                league_table = league_soup.find("table", {"class": "odds-data"})
                try:
                    for body in league_table("tbody"):
                        body.unwrap()
                    dfs = pd.read_html(str(league_table), flavor='bs4')
                    dfs[0].rename(columns={0: 'time', 1: 'team_name', 2: 'money_line', 3: 'handicap', 4: 'total_1', 5: 'total_2'}, inplace=True)
                    df_pinnacle = dfs[0]
                    df_pinnacle = df_pinnacle[pd.notnull(df_pinnacle['team_name'])]
                    league_name = league_link.split('/')[-1]
                    data_path = os.path.join(os.getcwd(), 'gamble_data', '{}.csv'.format(league_name))
                    df_pinnacle.to_csv(data_path, index=False)
                    url_visited.insert_url(league_url)
                except TypeError as te:
                    logging.info('error processing {} moving on error is {}'.format(league_url, te))
            else:
                logging.info('url {} is already present'.format(league_url))

    def scrap(self):
        self.find_odds_for_league()
