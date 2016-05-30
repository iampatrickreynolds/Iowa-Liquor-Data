from collections import namedtuple
import csv
import datetime
import re
import sys

from access import get_engine, get_session
from sql_models import Item, Store, Transaction, setup_database, stores_items


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

## Mappers for CSV data to sqlalchemy models. These greatly reduce memory
## overhead when building the database

ItemTuple = namedtuple("ItemTuple", [
    "number", "bottle_volume", "category", "category_name", "description",
    "pack", "vendor_name", "vendor_number"])

StoreTuple = namedtuple("StoreTuple", [
    "number", "address", "county", "county_number", "city", "location",
    "longitude", "latitude", "name", "zip_code"])

TransactionTuple = namedtuple("TransactionTuple", [
    "number", "bottle_cost", "bottle_retail", "bottles_sold", "date",
    "gallons_sold", "liters_sold", "total_sale", "store_number", "item_number"])


def item_tuple_from_row(row):
    item = ItemTuple(
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

def store_tuple_from_row(row):
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

    store = StoreTuple(
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

def transaction_tuple_from_row(row):
    # Watch out for '$' on price data
    transaction = TransactionTuple(
        number = row["Invoice/Item Number"],
        bottle_cost = float(row["State Bottle Cost"][1:]), # Remove '$' from front
        bottle_retail = float(row["State Bottle Retail"][1:]), # Remove '$' from front
        bottles_sold = int(row["Bottles Sold"]),
        date = parse_date(row["Date"]),
        gallons_sold = float(row["Volume Sold (Gallons)"]),
        liters_sold = float(row["Volume Sold (Liters)"]),
        total_sale = float(row["Sale (Dollars)"][1:]), # Remove '$' from front
        store_number = int(row["Store Number"]),
        item_number = int(row["Item Number"])
    )
    return transaction

def build_database(echo=False):
    """Construct the database from the large CSV file. To speed up the process
    we use the bulk insert methods of sqlalchemy, which achieve a 10x improvement
    in build time."""
    session = get_session(echo=echo)
    engine = get_engine(echo=echo)


    # First pass to extract objects without many-to-many relationship
    data = read_csv()
    items = dict()
    stores = dict()
    transactions = []
    for i, row in enumerate(data):
        # Create objects from the data in the row
        try:
            item = item_tuple_from_row(row)
            store = store_tuple_from_row(row)
            transaction = transaction_tuple_from_row(row)
        except ValueError as e:
            print(row)
            print(e)
            continue
        items[item.number] = item
        stores[store.number] = store
        transactions.append(transaction)
        if (i % 1000000) == 0:
            print(i)
            session.bulk_insert_mappings(Transaction,
                                 [t._asdict() for t in transactions])
            session.commit()
            transactions = []

    session.bulk_insert_mappings(Item,
                                 [item._asdict() for item in items.values()])
    session.bulk_insert_mappings(Store,
                                 [store._asdict() for store in stores.values()])
    session.bulk_insert_mappings(Transaction,
                                 [t._asdict() for t in transactions])
    session.commit()

    # Second pass to populate the m2m table for stores and items
    data = read_csv()
    m2m = []
    for i, row in enumerate(data):
        # Create objects from the data in the row
        try:
            transaction = transaction_tuple_from_row(row)
        except ValueError as e:
            print(row)
            print(e)
            continue
        m2m.append({"transaction_number": transaction.number,
                    "store_number": transaction.store_number,
                    "item_number": transaction.item_number})
        if (i % 1000000) == 0:
            print(i)
            engine.execute(stores_items.insert(), m2m)
            session.commit()
            m2m = []
    engine.execute(stores_items.insert(), m2m)
    session.commit()


def main(echo=False):
    setup_database(echo=echo, drop=False)
    build_database(echo=echo)

if __name__ == "__main__":
    try:
        if sys.argv[1] == "echo":
            echo = True
    except:
        echo = False
    main(echo=echo)
