from get_trading_data import get_data  # get data from NSE insider trading api
from combine_trade_data import combine_trade
import numpy as np
import pandas as pd
from datetime import datetime
import yfinance as yf
import os
import logging

logging.basicConfig(filename="data_update.log",
                    format='%(asctime)s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Fields to track
columns = ['date', 'ticker', 'company_name', 'person_name', 'person_category', 'number_of_securities',
           'transaction_type', 'shares_before_acq', 'shares_after_acq', 'pct_before_acq', 'pct_after_acq',
           'acq_mode', 'security_type', 'xbrl_link']

# insider_df = pd.DataFrame(columns=columns)
# insider_df.to_csv('insider.csv')
insider_df = pd.read_csv('insider.csv', index_col=0,
                         parse_dates=['date'])  # load csv file

insider_df['person_name'] = insider_df['person_name'].str.lower()
insider_df['person_category'] = insider_df['person_category'].str.lower()

data = get_data()  # fetch data obtained from API

if type(data) == str:

    # there was some error and we couldn't fetch data from the API
    logger.info('error fetching data')

else:

    logger.info('data fetched')

    # Fill the dataframe with the insider trades from the day
    insider_data = data['data']
    # we will fill this dataframe every day
    day_df = pd.DataFrame(columns=columns)

    for i in range(len(insider_data)):

        trade_data = insider_data[i]
        date = datetime.strptime(trade_data['date'], '%d-%b-%Y %H:%M')

        company_name = trade_data['company']
        ticker = trade_data['symbol']
        person_name = trade_data['acqName'].lower()

        if 'personCategory' in trade_data:
            person_category = trade_data['personCategory'].lower()
        else:
            person_category = '-'

        if type(trade_data['secAcq']) == str:
            number_of_securities = int(trade_data['secAcq'])
        else:
            number_of_securities = 0

        transaction_type = trade_data['tdpTransactionType']

        if (trade_data['befAcqSharesNo']) == 'Nil' or (trade_data['befAcqSharesNo']) == '-':
            shares_before_acq = 0
        else:
            shares_before_acq = int(trade_data['befAcqSharesNo'])

        if (trade_data['afterAcqSharesNo'] == 'Nil') or (trade_data['afterAcqSharesNo'] == '-'):
            shares_after_acq = 0
        else:
            shares_after_acq = int(trade_data['afterAcqSharesNo'])

        if (trade_data['befAcqSharesPer'] == 'Nil') or (trade_data['befAcqSharesPer'] == '-'):
            pct_before_acq = 0
        else:
            pct_before_acq = float(trade_data['befAcqSharesPer'])

        if (trade_data['afterAcqSharesPer'] == 'Nil') or (trade_data['afterAcqSharesPer'] == '-'):
            pct_after_acq = 0
        else:
            pct_after_acq = float(trade_data['afterAcqSharesPer'])

        acq_mode = trade_data['acqMode']
        security_type = trade_data['secType']
        xbrl_link = trade_data['xbrl']

        row = [date, ticker, company_name, person_name, person_category, number_of_securities, transaction_type,
               shares_before_acq, shares_after_acq, pct_before_acq, pct_after_acq, acq_mode, security_type, xbrl_link]

        day_df.loc[len(day_df.index)] = row

    # Post processing of the dataframe
    day_df = day_df.drop_duplicates()  # removing the duplicates

    # combine trades from same company same person
    day_df = combine_trade(day_df)

    # Merging with the insider dataframe
    # before doing this make sure there are no overlapping trades

    latest_date = insider_df['date'].max()
    day_df = day_df[day_df['date'] > latest_date].copy()

    logger.info(f'entries added: {len(day_df)}')
    insider_df = pd.concat([day_df, insider_df], ignore_index=True)
    insider_df.to_csv('insider.csv')
