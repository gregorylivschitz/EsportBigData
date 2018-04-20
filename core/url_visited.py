from models.models import Session
from models.models import UrlVisitedModel
from sqlalchemy import create_engine, exists
from sqlalchemy.orm import sessionmaker
from models.models import engine

class UrlVisited():
    
    def __init__(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()
        
    def insert_url(self, url_name):
        url = UrlVisitedModel(url_name=url_name)
        self.session.add(url)
        self.session.commit()

    def exists(self, url_name):
        (ret, ), = self.session.query(exists().where(UrlVisitedModel.url_name==url_name))
        return ret