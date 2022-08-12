import ast
import re
import pandas as pd
from datetime import datetime
import plotly.express as px
from config import Config

books = pd.read_csv(Config.BOOKS_FNAME)
custm = pd.read_csv(Config.CUSTOMERS_FNAME)
libs = pd.read_csv(Config.LIBRARIES_FNAME)

ch_outs = pd.read_parquet(Config.CHECKOUTS_FNAME_CLEAN)


## Rename columns for convinience

ch_outs = ch_outs.rename(columns={"id": "book_id"})

books = books.rename(columns={"id": "book_id"})

libs = libs.rename(columns={
    "id": "library_id",
    "street_address": "street_address_lib",
    "city": "city_lib",
    "name": "name_lib"
})

custm = custm.rename(columns={
    "id": "patron_id",
    "street_address": "street_address_customer",
    "city": "city_customer",
    "name": "name_custmer"
})


df = (
    ch_outs
    .merge(libs, on="library_id")
    .merge(books, on="book_id")
    .merge(custm, on="patron_id")
)


df.price = df.price.str.findall(r"\d\d?\d?\.?\d?\d?\d?").apply(lambda x: float(x[0]) if not pd.isnull(x) else x)
df.pages = df.pages.str.findall(r"\d\d?\d?").apply(lambda x: int(x[0]))
df.state = df.state.str.lower().str.strip()
df.city_customer = df.city_customer.str.lower().str.strip().str.replace(" +", " ")
df.gender = df.gender.str.lower().str.strip()
df.city_lib = df.city_lib.str.lower().str.strip().str.replace(" +", " ")
df.region = df.region.str.lower().str.strip()
df.education = df.education.str.lower().str.strip().str.replace(" +", " ")
df.occupation = df.occupation.str.lower().str.strip().str.replace(" +", " ")
df.name_lib = df.name_lib.str.lower().str.strip().str.replace(" +", " ")
df.publisher = df.publisher.str.lower().str.strip().str.replace(" +", " ")
df.categories[~df.categories.isna()] = df.categories[~df.categories.isna()].apply(ast.literal_eval).apply(lambda x:x[0]).values
df.publishedDate[~df.publishedDate.isna()] = df.publishedDate[~df.publishedDate.isna()].apply(lambda x: re.findall(r"\d\d\d\d", x)[0])

df["publishedDate"] = pd.to_datetime(
    df["publishedDate"], infer_datetime_format=True
)

df["birth_date"] = pd.to_datetime(
    df["birth_date"], infer_datetime_format=True
)

df["book_age_days"] = (df.date_checkout - df["publishedDate"]).apply(lambda x: x.days)
df["customer_age_days"] = (df.date_checkout - df["birth_date"]).apply(lambda x: x.days)


df.to_parquet(Config.PREPARED_DATASET_FNAME)