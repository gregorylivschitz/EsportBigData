import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd

class PinnacleScrapper():

    def __init__(self, game_name, base_url):
        self.game_name = game_name
        self.base_url = base_url
        self.find_odds_for_league()


    def find_leagues(self):
        pinnicale_base_url = requests.get(self.base_url)
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
        league_links = self.find_leagues()
        for league_link in league_links:
            league_url = '{}{}'.format(self.base_url, league_link)
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            browser = webdriver.Chrome(chrome_options=options)
            browser.get(league_url)
            html = browser.page_source
            league_soup = BeautifulSoup(html, 'html.parser')
            league_table = league_soup.find("table", {"class": "odds-data"})
            for body in league_table("tbody"):
                body.unwrap()
            dfs = pd.read_html(str(league_table), flavor='bs4')
            print(dfs[0].head())
