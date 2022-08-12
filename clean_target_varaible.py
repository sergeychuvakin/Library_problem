## Fix all mistakes in target variable

from datetime import datetime

import pandas as pd

from config import Config

first_pattern = r"2\d\d\d\-\d\d-\d\d"
second_pattern = r"2\d\d\d\/\d\d/\d\d"
third_pattern = r"2\d\d\d\d\d\d\d"
fourth_pattern = r"1\d\d\d\-\d\d-\d\d"
fifth_pattern = r"2\d\d\d\s\d\d\s\d\d"
sixth_pattern = r"2\d\d\d\|\d\d\|\d\d"


ch_outs = pd.read_csv(Config.CHECKOUTS_FNAME)

# `first_pattern` I'll pick as a reference. So let's fix all the other. But before moving forward let's drop NA because we have nothing to do with it. Also I'll remove extra \% sign and strip strigs.

ch_outs = ch_outs.dropna(subset=["date_checkout", "date_returned"])

ch_outs.date_checkout = ch_outs.date_checkout.str.replace("%", "")
ch_outs.date_returned = ch_outs.date_returned.str.replace("%", "")

ch_outs.date_checkout = ch_outs.date_checkout.str.strip()
ch_outs.date_returned = ch_outs.date_returned.str.strip()


#### 2 pattern mistake

ch_outs.date_checkout[
    ch_outs.date_checkout.str.match(second_pattern)
] = ch_outs.date_checkout[ch_outs.date_checkout.str.match(second_pattern)].str.replace(
    "/", "-"
)

ch_outs.date_returned[
    ch_outs.date_returned.str.match(second_pattern)
] = ch_outs.date_returned[ch_outs.date_returned.str.match(second_pattern)].str.replace(
    "/", "-"
)


#### 3 pattern mistake

ch_outs.date_checkout[
    ch_outs.date_checkout.str.match(third_pattern)
] = ch_outs.date_checkout[ch_outs.date_checkout.str.match(third_pattern)].apply(
    lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d")
)

ch_outs.date_returned[
    ch_outs.date_returned.str.match(third_pattern)
] = ch_outs.date_returned[ch_outs.date_returned.str.match(third_pattern)].apply(
    lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d")
)


#### 4 pattern mistake

# The most difficult one - it seems that 18** year in fact should be 2018. Let's assume like there were these kind of typos.

ch_outs.date_checkout[
    ch_outs.date_checkout.str.match(fourth_pattern)
] = ch_outs.date_checkout[ch_outs.date_checkout.str.match(fourth_pattern)].str.replace(
    r"1\d\d\d", r"2018"
)

ch_outs.date_returned[
    ch_outs.date_returned.str.match(fourth_pattern)
] = ch_outs.date_returned[ch_outs.date_returned.str.match(fourth_pattern)].str.replace(
    r"1\d\d\d", r"2018"
)


#### 5 pattern mistake

ch_outs.date_checkout[
    ch_outs.date_checkout.str.match(fifth_pattern)
] = ch_outs.date_checkout[ch_outs.date_checkout.str.match(fifth_pattern)].str.replace(
    "\s", "-"
)

ch_outs.date_returned[
    ch_outs.date_returned.str.match(fifth_pattern)
] = ch_outs.date_returned[ch_outs.date_returned.str.match(fifth_pattern)].str.replace(
    "\s", "-"
)


#### 6 pattern mistake

ch_outs.date_checkout[
    ch_outs.date_checkout.str.match(sixth_pattern)
] = ch_outs.date_checkout[ch_outs.date_checkout.str.match(sixth_pattern)].str.replace(
    "|", "-"
)

ch_outs.date_returned[
    ch_outs.date_returned.str.match(sixth_pattern)
] = ch_outs.date_returned[ch_outs.date_returned.str.match(sixth_pattern)].str.replace(
    "|", "-"
)


#### 7 pattern mistake

# Logically it's impossible that difference is negative so let's remove that cases.


ch_outs["date_checkout"] = pd.to_datetime(
    ch_outs["date_checkout"], infer_datetime_format=True
)
ch_outs["date_returned"] = pd.to_datetime(
    ch_outs["date_returned"], infer_datetime_format=True
)

ch_outs["delay_days"] = (ch_outs["date_returned"] - ch_outs["date_checkout"]).apply(
    lambda x: x.days
)

ch_outs = ch_outs[ch_outs.delay_days > 0].reset_index(drop=True)


### save final data

ch_outs["is_delayed"] = ch_outs.delay_days > Config.DELAY_DAY

ch_outs.to_parquet(Config.CHECKOUTS_FNAME_CLEAN)
