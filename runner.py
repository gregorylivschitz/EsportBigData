from core.scraper.pinnacle_scraper import PinnacleScrapper

def run():
    pinnacle_scraper = PinnacleScrapper('dota-2', 'https://www.pinnacle.com')

if __name__ == '__main__':
    run()