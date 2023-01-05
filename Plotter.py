import matplotlib.pyplot as plt
import matplotlib.dates as mpdates
from mplfinance.original_flavor import candlestick_ohlc
import pandas as pd
class Plotter:
    """
    This is the class aiming for plotting

    Attributes:
    titile(str): The string title of the plot
    """
    def __init__(self, title=''):
        self.title = title
    
    def plot(self, df):
        plt.style.use('dark_background')

        # extracting Data for plotting
        df = df[['trade_date', 'open', 'high', 'low', 'close']]

        # convert into datetime object
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['trade_date'] = df['trade_date'].map(mpdates.date2num)
        
        # creating Subplots
        fig, ax = plt.subplots()
        
        # plotting the data
        candlestick_ohlc(
            ax, 
            df.values, 
            width = 0.6, 
            colorup = 'green', 
            colordown = 'red',
            alpha = 0.8)
        # allow grid
        ax.grid(True)
        
        # Setting labels
        ax.set_xlabel('Date')
        ax.set_ylabel('Price')
        
        plt.savefig("mygraph.png")