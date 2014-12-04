from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

class SObject(Base):
    __tablename__ = 'sobject'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)

class Relationship(Base):
    __tablename__ = 'relationship'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    sobject_id = Column(Integer, ForeignKey('sobject.id'))
    master_detail = Column(String(1), default=0, nullable=False) #really should be boolean
    field = Column(String(250), nullable=False)

class RecordType(Base):
    __tablename__ = 'recordtype'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    sf_id = Column(String(18), nullable=False)
    sobject_id = Column(Integer, ForeignKey('sobject.id'))
    amount = Column(Integer)

class Owner(Base):
    __tablename__ = 'owner'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    sf_id = Column(String(18), nullable=False)
    sobject_id = Column(Integer, ForeignKey('sobject.id'))
    amount = Column(Integer)