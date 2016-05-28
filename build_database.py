
import csv
import datetime
import re
import sys

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from access import get_engine, get_session
from sql_models import Item, Store, Transaction, setup_database


location_regex = re.compile("(.*?)\((.*?),\s(.*?)\)", re.MULTILINE | re.DOTALL)

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

## Mappers for CSV data to sqlalchemy models

def create_item_from_row(row):
    item = Item(
        number = int(row["Item Number"]),
        bottle_volume = int(row["Bottle Volume (ml)"]),
        category = row["Category"],
        category_name = row["Category Name"],
        description = row["Item Description"],
        pack = int(row["Pack"]),
        vendor_name = row["Vendor Name"],
        vendor_number = int(row["Vendor Number"]),
    )
    return item

def create_store_from_row(row):
    # Sometimes the County number is missing
    try:
        county_number = int(row["County Number"])
    except ValueError:
        county_number = None

    # The location cell needs to be further parsed
    try:
        _, longitude, latitude = location_regex.match(row["Store Location"]).groups()
    except AttributeError:
        longitude = None
        latitude = None

    # The Dunlap zip code is sometimes incorrect
    zip_code = row["Zip Code"]
    if zip_code == "712-2":
        zip_code = 51529

    store = Store(
        number = int(row["Store Number"]),
        address = row["Address"],
        county = row["County"],
        county_number = county_number,
        city = row["City"],
        location = row["Store Location"],
        longitude = longitude,
        latitude = latitude,
        name = row["Store Name"],
        zip_code = int(zip_code)
    )
    return store


def parse_date(date_string):
    dt = datetime.datetime.strptime(date_string, "%m/%d/%Y")
    return dt.date()

def create_transaction_from_row(row):
    # Watch out for '$' on price data
    transaction = Transaction(
        number = row["Invoice/Item Number"],
        bottle_cost = float(row["State Bottle Cost"][1:]), # Remove '$' from front
        bottle_retail = float(row["State Bottle Retail"][1:]), # Remove '$' from front
        bottles_sold = int(row["Bottles Sold"]),
        date = parse_date(row["Date"]),
        gallons_sold = float(row["Volume Sold (Gallons)"]),
        liters_sold = float(row["Volume Sold (Liters)"]),
        total_sale = float(row["Sale (Dollars)"][1:]) # Remove '$' from front
    )
    return transaction


def build_database(echo=False):
    session = get_session()

    data = read_csv()
    for i, row in enumerate(data):
        # Create objects from the data in the row
        try:
            item = create_item_from_row(row)
            store = create_store_from_row(row)
            transaction = create_transaction_from_row(row)
        except ValueError as e:
            print(row)
            print(e)
            continue
        transaction.item = item
        transaction.store = store
        store.items.append(item)
        session.merge(item)
        session.merge(store)
        session.merge(transaction)
        if (i % 1000) == 0:
            session.commit()
    session.commit()


def main(echo=False):
    setup_database(echo=echo, drop=True)
    build_database(echo=echo)

if __name__ == "__main__":
    try:
        if sys.argv[1] == "echo":
            echo = True
    except:
        echo = False
    main(echo=echo)
