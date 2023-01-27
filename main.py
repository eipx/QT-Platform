import pandas as pd
import src.Pipeline as Pipeline
from src.credentials import *
import backtrader as bt
import backtrader.indicators as btind
import tushare as ts
from datetime import datetime

class MyStrategy(bt.Strategy):
    params = (
        ('sma_period', 100),
    )
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.sma_period)
    
    def next(self):
        if self.data.close[0] > self.sma[0]:
            self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.sell()

# Create an instance of Cerebro
cerebro = bt.Cerebro()

# Add the Strategy 
cerebro.addstrategy(MyStrategy)

# Load the data
ts.set_token(my_token)
pro = ts.pro_api()
pipeline = Pipeline.IndexPipeline('399300.SZ', '20170901', pro)
df = pipeline.get_stock('000001.SZ', start_date='20170901', end_date='20230101')
df = df.rename(columns={'vol':'volume'})
df['openinterest'] = 0
df.index = pd.to_datetime(df.trade_date)
df = df[['open', 'high', 'low', 'close', 'volume', 'openinterest']]
fromdate = datetime(2017, 9, 1)
todate = datetime(2023, 1, 1)
print(df)
data = bt.feeds.PandasData(dataname=df, fromdate=fromdate, todate=todate)

# Add the data to cerebro
cerebro.adddata(data)

# Run the strategy
cerebro.run()

cerebro.plot()