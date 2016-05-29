
from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Index, Integer, String, Table, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from access import get_engine, get_session


Base = declarative_base()


def setup_database(echo=False, drop=True):
    """Setup the database, dropping any existing tables by default."""
    engine = get_engine(echo=echo)
    session = get_session(echo=echo)
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


# Manual relationship

stores_items = Table('stores_items', Base.metadata,
    Column('transaction_number', String, primary_key=True, index=True),
    Column('store_number', Integer, ForeignKey('Store.number'), index=True),
    Column('item_number', Integer, ForeignKey('Item.number'), index=True)
)


class Store(Base):
    __tablename__ = "Store"

    number = Column(Integer, primary_key=True, index=True)

    address = Column(String)
    city = Column(String, index=True)
    county = Column(String)
    county_number = Column(Integer, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    location = Column(String) # Usually address + lat and long
    name = Column(String, index=True)
    zip_code = Column(Integer, index=True)

    transactions = relationship("Transaction", back_populates="store")
    items = relationship("Item", secondary=stores_items, back_populates="stores")

    __mapper_args__ = {
        "order_by": [county_number, zip_code]
    }


class Item(Base):
    __tablename__ = "Item"

    number = Column(Integer, primary_key=True, index=True)

    bottle_volume = Column(Float) # milliliters
    category = Column(String, index=True)
    category_name = Column(String)
    description = Column(String)
    pack = Column(Integer)
    vendor_name = Column(String)
    vendor_number = Column(Integer, index=True)

    transactions = relationship("Transaction", back_populates="item")
    stores = relationship("Store", secondary=stores_items, back_populates="items")

    __mapper_args__ = {
        "order_by": [category]
    }


class Transaction(Base):
    __tablename__ = "Transaction"

    number = Column(String, primary_key=True, index=True)

    bottle_cost = Column(Float)
    bottle_retail = Column(Float)
    bottles_sold = Column(Integer)
    date = Column(Date)
    item_number = Column(Integer, ForeignKey("Item.number"))
    gallons_sold = Column(Float)
    liters_sold = Column(Float)
    store_number = Column(Integer, ForeignKey("Store.number"))
    total_sale = Column(Float) # dollars

    store = relationship("Store", back_populates="transactions",
                                order_by="Store.number")
    item = relationship("Item", back_populates="transactions",
                                order_by="Item.number")

    __mapper_args__ = {
        "order_by": [desc(date)]
    }
