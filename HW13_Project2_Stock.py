import yfinance as yf
from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime
#import datetime as dt



beginDate = '2020-01-01'
endDate = '2020-12-08'
ticker_list=['MSFT','TSLA','AMZN','AAPL','JPM','WORK','CRM','ZM','PTON','DIS','MCD','PRTY','CSCO','GOOGL','ORCL']



# Report on stocks' Adj Close and Volume

counter = 0
for ticker in ticker_list:
    print(f'Input to Stock Analysis: {ticker}')
    stockData = yf.download(ticker, start=beginDate, end=endDate)
    stockData['Ticker'] = ticker
    stockReport = pd.DataFrame(stockData, columns= ['Ticker','Adj Close','Volume'])
    stockReport.to_csv(f'output\{ticker}Report.csv')
    if counter < 1:
        mergedReport = stockReport
    else:
        mergedReport = pd.merge(mergedReport, stockReport, on=['Date', 'Ticker','Adj Close', 'Volume'], how='outer')
    counter = counter + 1
print(mergedReport)
mergedReport.to_csv('input\StockReport.csv')



# Report on stocks' noise

counter2 = 0
for stock in ticker_list:
    print(f'Input to Trend Analysis: {stock}')
    pytrend = TrendReq(hl='en-US', tz=360)
    pytrend.build_payload(kw_list=[stock], timeframe=beginDate + ' ' + endDate, geo='US')
    df = pytrend.interest_over_time()
    df['Noise'] = df[stock]
    df[stock]=stock
    df.index.names = ['Date']
    df.columns = ['Ticker','isPartial','Noise']
    df.to_csv(f'output\{stock}Noise.csv')
    if counter2 < 1:
            mergedNoise = df
    else:
            mergedNoise = pd.merge(mergedNoise, df, on=['Date', 'Ticker', 'isPartial', 'Noise'], how= 'outer')  
    counter2 = counter2 + 1
print(mergedNoise)
mergedNoise.to_csv('input\StockNoise.csv')



# Convert Date Valuesand merge based on weekly values

# import the trends and stocks csvs
trends = pd.read_csv('input\StockNoise.csv')
stocks = pd.read_csv('input\StockReport.csv')
twint = pd.read_csv('input/twintReport.csv')

# Trend dates are all Sunday, Stock dates are all weekdays M-F so they dont directly overlap to allow a merge
# Convert date values to datetime and generate an isocalendar date tuple (year, wk of year, day of week) to merge data sets that dont share the same date values
# based on their wk of the year value instead
trends['Date'] = pd.to_datetime(trends['Date']).dt.strftime('%Y-%m-%d')
iso_dates_yr = []
iso_dates_wk = []
for row in range(0,len(trends)):
    date = trends.loc[row]['Date']
    dt = datetime.strptime(date, '%Y-%m-%d')
# Create the Timestamp object
    date = pd.Timestamp(year = dt.year,  month = dt.month, day = dt.day)
    date2 = date.isocalendar()
    iso_dates_yr.append(date2[0])
    iso_dates_wk.append(date2[1])
# add the isoclaendar dates to the data frame as a new column
trends['isocal_yr'] = iso_dates_yr
trends['isocal_wk'] = iso_dates_wk

# repeat process for the stock data
stocks['Date'] = pd.to_datetime(stocks['Date']).dt.strftime('%Y-%m-%d')
iso_dates_yr = []
iso_dates_wk = []
for row in range(0,len(stocks)):
    date = stocks.loc[row]['Date']
    dt = datetime.strptime(date, '%Y-%m-%d')
# Create the Timestamp object
    date = pd.Timestamp(year = dt.year,  month = dt.month, day = dt.day)
    date2 = date.isocalendar()
    iso_dates_yr.append(date2[0])
    iso_dates_wk.append(date2[1])
stocks['isocal_yr'] = iso_dates_yr
stocks['isocal_wk'] = iso_dates_wk

# repeat process for the twint data
twint['Date'] = pd.to_datetime(twint['Date']).dt.strftime('%Y-%m-%d')
iso_dates_yr = []
iso_dates_wk = []
for row in range(0,len(twint)):
    date = twint.loc[row]['Date']
    dt = datetime.strptime(date, '%Y-%m-%d')
# Create the Timestamp object
    date = pd.Timestamp(year = dt.year,  month = dt.month, day = dt.day)
    date2 = date.isocalendar()
    iso_dates_yr.append(date2[0])
    iso_dates_wk.append(date2[1])
twint['isocal_yr'] = iso_dates_yr
twint['isocal_wk'] = iso_dates_wk

# group the stock data by ticker symol and week using aggregate function to create weekly Open, Close, High, Low, and total trading volume summary data
stocks_group = stocks.groupby(['Ticker','isocal_yr','isocal_wk']).agg({'Date':'last', 'Adj Close':'last', 'Volume':'sum'})
twint_group = twint.groupby(['Ticker','isocal_yr','isocal_wk']).agg({'Date':'last', 'Tweet Count':'sum'})
#merge the data tables on isocalendar date and ticker symbol
stock_twint = pd.merge(twint_group, stocks_group, how='inner', on=['Ticker','isocal_yr', 'isocal_wk'], suffixes=(' TNoise', ' Stock'))
data_table = pd.merge(trends, stock_twint, how='inner', on=['Ticker','isocal_yr', 'isocal_wk'])
finalReport = pd.DataFrame(data_table, columns= ['Ticker','isocal_yr', 'isocal_wk','Noise','Tweet Count','Adj Close','Volume', 'Date','Date TNoise','Date Stock'])
finalReport.to_csv('output\FinalReport.csv')
print(finalReport)
#The final table has the weekly interest trend and the weekly stock closing value

