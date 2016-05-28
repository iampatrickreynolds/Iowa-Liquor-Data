
import csv
import datetime
import os
import sys

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from access import get_engine, get_session
from sql_models import Item, Store, Transaction


def read_csv(filename="Iowa_Liquor_Sales.csv"):
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)
        # The headers are the first row
        headers = None
        for i, row in enumerate(reader):
            if i == 0:
                headers = [x.strip() for x in row]
                continue
            yield dict(zip(headers, row))


def setup_database(echo=False, drop=True):
    """Setup the database, dropping any existing tables by default."""
    engine = get_engine()
    session = get_session()
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def create_item_from_row(row):
    item = Item(
        number = row["Item Number"],
        bottle_volume = row["Bottle Volume (ml)"],
        category = row["Category"],
        category_name = row["Category Name"],
        description = row["Item Description"],
        pack = row["Pack"],
        vendor_name = row["Vendor Name"],
        vendor_number = row["Vendor Number"],
    )
    return item


def create_store_from_row(row):
    store = Store(
        number = row["Store Number"],
        address = row["Address"],
        county = row["County"],
        county_number = row["County Number"],
        city = row["City"],
        location = row["Store Location"],
        name = row["Store Name"],
        zip_code = row["Zip Code"]
    )
    return store

def parse_date(date_string):
    date = datetime.Date.strptime(date_string, "%m/%d/%Y")
    return date

def create_transaction_from_row(row):
    transaction = Transaction(
        number = row["Invoice/Item Number"],
        bottle_cost = row["State Bottle Cost"],
        bottle_retail = row["State Bottle Retail"],
        bottles_sold = row["Bottles Sold"],
        date = parse_date(row["Date"]),
        gallons_sold = row["Volume Sold (Gallons)"],
        liters_sold = row["Volume Sold (Liters)"],
        total_sale = row["Sale (Dollars)"]
    )
    return transaction



def build_database(echo=False):
    session = get_session()

    data = read_csv()
    for i, row in enumerate(data):
        # Create objects from the data in the row
        item = create_item_from_row(row)
        store = create_store_from_row(row)
        transaction = create_transaction_from_row(row)
        transaction.item = item
        transaction.store = store
        store.items.append(item)
        item.stores.append(store)
        session.merge(item)
        session.merge(store)
        session.merge(transaction)
        if (i % 1000) == 0:
            session.commit()
    session.commit()


if __name__ == "__main__":
    echo = False
    try:
        if sys.argv[1] == "echo":
            echo = True
    except:
        pass
    build_database(echo=echo)
