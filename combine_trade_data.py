import pandas as pd
import numpy as np


def combining_trade_data(group):

    combined_row = group.iloc[0].copy()

    buy_total = group[group['transaction_type']
                      == 'Buy']['number_of_securities'].sum()
    sell_total = group[group['transaction_type']
                       == 'Sell']['number_of_securities'].sum()

    if buy_total == 0 and sell_total == 0:
        return group  # returns a dataframe

    elif buy_total >= sell_total:

        combined_row['transaction_type'] = 'Buy'
        combined_row['number_of_securities'] = buy_total - sell_total
        combined_row['shares_before_acq'] = group['shares_before_acq'].min()
        combined_row['pct_before_acq'] = group['pct_before_acq'].min()
        combined_row['shares_after_acq'] = group['shares_after_acq'].max()
        combined_row['pct_after_acq'] = group['pct_after_acq'].max()

    else:

        combined_row['transaction_type'] = 'Sell'
        combined_row['number_of_securities'] = sell_total - buy_total
        combined_row['shares_before_acq'] = group['shares_before_acq'].max()
        combined_row['pct_before_acq'] = group['pct_before_acq'].max()
        combined_row['shares_after_acq'] = group['shares_after_acq'].min()
        combined_row['pct_after_acq'] = group['pct_after_acq'].min()

    # converting the series to dataframe for consistency
    return combined_row.to_frame().T


def combine_trade(df):
    df = df.groupby(['company_name', 'person_name']).apply(
        combining_trade_data)  # combining trades for the same person
    df = df.reset_index(drop=True)
    df = df.sort_values('date', ascending=False)
    df = df.reset_index(drop=True)
    return df
