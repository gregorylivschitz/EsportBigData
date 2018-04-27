from sqlalchemy import Column, Table, Integer, ForeignKey, create_engine, String, Boolean, Numeric, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

engine = create_engine('sqlite:///esport_bigdata.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class TimestampMixin(object):
    created_date = Column(DateTime, default=func.now())


class UrlVisitedModel(Base, TimestampMixin):
    """UrlVisitedModel Object"""
    __tablename__ = 'url_visited'
    id = Column(Integer, primary_key=True)
    url_name = Column(String)

class MmrProcessModel(Base, TimestampMixin):
    """MmrProcess Object"""
    __tablename__ = 'mmr_process'
    id = Column(Integer, primary_key=True)
    mmr_start = Column(Integer)
    mmr_end = Column(Integer)


Base.metadata.create_all(engine)