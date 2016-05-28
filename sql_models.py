
from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Index, Integer, String, Table, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship


Base = declarative_base()



stores_items = Table('stores_items', Base.metadata,
    Column('store_number', Integer, ForeignKey('Store.number'), index=True),
    Column('item_number', Integer, ForeignKey('Item.number'), index=True)
)



class Store(Base):
    __tablename__ = "Store"

    number = Column(Integer, primary_key=True)

    address = Column(String)
    city = Column(String)
    county = Column(String)
    county_number = Column(Integer)
    location = Column(String) # Usually same as address
    name = Column(String)
    zip_code = Column(Integer)

    transactions = relationship("Transactions", back_populates="stores",
                                order_by="Transaction.date")
    items = relationship("Item", secondary=stores_items, back_populates="stores")

    __mapper_args__ = {
        "order_by": [number, county_number, zip_code]
    }


class Item(Base):
    __tablename__ = "Item"

    number = Column(Integer, primary_key=True)

    bottle_volume = Column(Float) # milliliters
    category = Column(String)
    category_name = Column(String)
    description = Column(String)
    pack = Column(Integer)
    vendor_name = Column(String)
    vendor_number = Column(Integer)

    transactions = relationship("Transactions", back_populates="items",
                                order_by="Transaction.date")
    stores = relationship("Store", secondary=stores_items, back_populates="items")

    __mapper_args__ = {
        "order_by": [number, category, vendor_number]
    }



class Transaction(Base):
    __tablename__ = "Transaction"

    number = Column(Integer, primary_key=True)

    bottle_cost = Column(Float)
    bottle_retail = Column(Float)
    bottles_sold = Column(Integer)
    date = Column(Date)
    item = Column(ForeignKey("Item.number"))
    gallons = Column(Float)
    liters = Column(Float)
    store = Column(ForeignKey("Store.number"))
    total_sale = Column(Float) # dollars

    stores = relationship("Store", back_populates="Transactions",
                                order_by="Store.number")
    stores = relationship("Items", back_populates="Transactions",
                                order_by="Item.number")

    __mapper_args__ = {
        "order_by": [datetime, item]
    }


