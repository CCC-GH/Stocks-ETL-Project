# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 17:41:40 2020

@author: Mason
"""

import twint
import pandas
import nest_asyncio

nest_asyncio.apply()

# LIST OF TICKER SYMBOLS TO SEARCH BY #
tickers = ['ZM','PTON','DIS','MCD','PRTY','CSCO','GOOGL','ORCL', 'WORK']

# SCRAPED = ['MSFT','TSLA','AMZN','AAPL','CRM']

# CREATES EMPTY DATAFRAME TO BE APPENDED TO #
agg_df = pandas.DataFrame(columns = ['date', 'search', 'id'])

for ticker in tickers:
    # CONFIG FOR SCRAPE, BASED ON TICKER SYMBOL #
    c = twint.Config()
    c.Search = ticker
    c.Since = '2020-11-01' # REASONABLE START DATE (THIS TAKES A WHILE TO COOK)
    c.Hide_output = True
    c.Pandas = True
    
    # RUNS THE SCRAPE #
    twint.run.Search(c)
    
    # SAVES THE SCRAPED DATA TO A DATAFRAME #
    Tweets_df = twint.storage.panda.Tweets_df
    
    # CLEANS UP DATAFRAME #
    Tweets_df = Tweets_df.drop(columns=['conversation_id', 'created_at', 'timezone', 'place', 'tweet', 'language', 'hashtags',
                                    'cashtags', 'user_id', 'user_id_str', 'username', 'name', 'day', 'hour', 'link', 'urls', 'photos',
                                    'video', 'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url', 'near',
                                    'geo', 'source', 'user_rt_id', 'user_rt', 'retweet_id', 'reply_to', 'retweet_date', 'translate', 
                                    'trans_src', 'trans_dest'])
    Tweets_df['date'] = pandas.to_datetime(Tweets_df['date']).dt.date
    
    # SAVES DATAFRAME TO CSV #
    Tweets_df.to_csv("./ticker_data/" + ticker + ".csv")
    
    # APPENDS TICKER DATAFRAMES TO AGGREGATE DATAFRAME #
    agg_df = agg_df.append(Tweets_df, sort = False)
    
    
# RENAMES COLUMN NAMES OF AGGREGATED DATAFRAME #    
agg_df.columns = ['Date', 'Ticker', 'TweetId']

# GROUPS AGGREGATED DATAFRAME BY TICKER AND DATE #
agg_df = agg_df.groupby(['Ticker', 'Date']).count()

# WRITES AGGREGATED DATAFRANE TO A CSV #
agg_df.to_csv("./ticker_data/agg_data.csv")