from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy import create_engine

Base = declarative_base()

class SObject(Base):
    __tablename__ = 'sobject'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    amount = Column(Integer, nullable=True)
    #amount_requested(Integer, nullable=True)
    fields = Column(Text, nullable=True)
    extract_order_id = Column(Integer, ForeignKey('extract_order.id'), nullable=True)
    record_types = relationship('RecordType', backref='sobject')
    children = relationship('Relationship', backref='parent', foreign_keys="[Relationship.child_id]")
    parents = relationship('Relationship', backref='child', foreign_keys="[Relationship.parent_id]")

    def build_query(self):
        return 'SELECT %s FROM %s' % (self.fields, self.name)

class Field(Base):
    __tablename__ = 'field'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    type = Column(String(80), nullable=False)
    sobject_id = Column(Integer, ForeignKey('sobject.id'))

class Relationship(Base):
    __tablename__ = 'relationship'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    parent_id = Column(Integer, ForeignKey('sobject.id', ondelete='CASCADE'))
    child_id = Column(Integer, ForeignKey('sobject.id', ondelete='CASCADE'))
    master_detail = Column(String(1), default=0, nullable=False) #really should be boolean
    field = Column(String(250), nullable=False)

class ExtractOrder(Base):
    __tablename__ = 'extract_order'

    id = Column(Integer, primary_key=True)
    position = Column(Integer, nullable=False)

class RecordType(Base):
    __tablename__ = 'recordtype'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    sf_id = Column(String(18), nullable=False)
    sobject_id = Column(Integer, ForeignKey('sobject.id', ondelete='CASCADE'))
    amount = Column(Integer)
    #amount_requested = Column(Integer)

class Owner(Base):
    __tablename__ = 'owner'

    id = Column(Integer, primary_key=True)
    is_active = Column(Integer, nullable=False)
    sf_id = Column(String(18), nullable=False)
    #amount = Column(Integer)
    #amount_requested = Column(Integer)

class SObjectOwner(Base):
    __tablename__ = 'sobject_owner'

    id = Column(Integer, primary_key=True)
    sobject_id = Column(Integer, ForeignKey('sobject.id', ondelete='CASCADE'))
    owner_id = Column(Integer, ForeignKey('owner.id'))
    amount = Column(Integer)

