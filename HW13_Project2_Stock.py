# Report on stocks' Adj Close and Volume
import yfinance as yf
import pandas as pd
filePath = '\\Users\coffm\GitHub\Stocks-ETL-Project'
beginDate = '2020-01-01'
endDate = '2020-12-08'
ticker_list=['MSFT','TSLA','AMZN','AAPL','JPM','WORK','CRM','ZM','PTON','DIS','MCD','PRTY','CSCO','GOOGL','ORCL']
counter = 0
for ticker in ticker_list:
    print(f'Input to Stock Analysis: {ticker}')
    stockData = yf.download(ticker, start=beginDate, end=endDate)
    stockData['Ticker'] = ticker
    stockReport = pd.DataFrame(stockData, columns= ['Ticker','Adj Close','Volume'])
    stockReport.to_csv(f'{filePath}\output\{ticker}Report.csv')
    if counter < 1:
        mergedReport = stockReport
    else:
        mergedReport = pd.merge(mergedReport, stockReport, on=['Date', 'Ticker','Adj Close', 'Volume'], how='outer')
    counter = counter + 1
print(mergedReport)
mergedReport.to_csv(filePath + '\input\StockReport.csv')


# Report on stocks' noise
from pytrends.request import TrendReq
import pandas as pd
filePath = '\\Users\coffm\GitHub\Stocks-ETL-Project'
start_time = '2020-01-01'
end_time = '2020-12-08'
stock_list = ['MSFT','TSLA','AMZN','AAPL','JPM','WORK','CRM','ZM','PTON','DIS','MCD','PRTY','CSCO','GOOGL','ORCL']
counter2 = 0
for stock in stock_list:
    print(f'Input to Trend Analysis: {stock}')
    pytrend = TrendReq(hl='en-US', tz=360)
    pytrend.build_payload(kw_list=[stock], timeframe=start_time + ' ' + end_time, geo='US')
    df = pytrend.interest_over_time()
    df['Noise'] = df[stock]
    df[stock]=stock
    df.index.names = ['Date']
    df.columns = ['Ticker','isPartial','Noise']
    df.to_csv(f'{filePath}\output\{stock}Noise.csv')
    if counter2 < 1:
            mergedNoise = df
    else:
            mergedNoise = pd.merge(mergedNoise, df, on=['Date', 'Ticker', 'isPartial', 'Noise'], how= 'outer')  
    counter2 = counter2 + 1
print(mergedNoise)
mergedNoise.to_csv(filePath + '\input\StockNoise.csv')

import pandas as pd
from datetime import datetime
import datetime as dt
filePath = '\\Users\coffm\GitHub\Stocks-ETL-Project'
# import the trends and stocks csvs
trends = pd.read_csv(f'{filePath}\input\StockNoise.csv')
stocks = pd.read_csv(f'{filePath}\input\StockReport.csv')
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
#rename the symbol column to ticker to match the trends table
stocks.rename(columns = {'Symbol':'Ticker'}, inplace = True)
# group the stock data by ticker symol and week using aggregate function to create weekly Open, Close, High, Low, and total trading volume summary data
stocks_group = stocks.groupby(['Ticker','isocal_yr','isocal_wk']).agg({'Date':'last','Open':'first','High': 'max', 'Low': 'min', 'Close':'last', 'Adj Close':'last', 'Volume':'sum'})
#merge the data tables on isocalendar date and ticker symbol
data_table = pd.merge(trends, stocks_group, how='inner', on=['Ticker','isocal_yr', 'isocal_wk'], suffixes=(' Interest', ' Value'))
finalReport = pd.DataFrame(data_table, columns= ['Ticker','isocal_yr', 'isocal_wk','Interest Over Time','Adj Close','Volume', 'Date Interest','Date Value'])
finalReport.to_csv(f'{filePath}\output\'finalReport.csv')
finalReport
#The final table has the weekly interest trend and the weekly stock closing value

