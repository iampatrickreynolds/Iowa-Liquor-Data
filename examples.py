from access import get_session
from sql_models import Item, Store, Transaction

session = get_session()

# See all Stores

stores = session.query(Store).all()
print(len(stores))

# See all Items with pack == 6

items = session.query(Item).filter(Item.pack == 6).all()
print(len(items))

# Grab one transaction from a particular day

import datetime

date = datetime.date(2015, 1, 7)
transaction = session.query(Transaction).filter(
    Transaction.date == date).first()  # or .one() if sorted
print(transaction.date, transaction.total_sale, transaction.store.name)


# For a random store, get all the transactions and sort by date
import random
store = random.choice(stores)
transactions = store.transactions
from operator import attrgetter
transactions.sort(key=attrgetter("date"))

# Build a dataframe for the transactions per day for all stores
from collections import defaultdict
import pandas as pd
data = []

# Summarize the sales data per store and day
# This one will take a while!
for store in stores:
    d = defaultdict(float)
    for tr in store.transactions:
        d[tr.date.isoformat()] += tr.total_sale
    for date_str, sales in d.items():
        data.append((store.number, store.zip_code, date_str, sales))
columns = ["Store Number", "Zip Code", "Date", "Sales"]
df = pd.DataFrame(data, columns=columns)
df.to_csv("sales_data.csv")


