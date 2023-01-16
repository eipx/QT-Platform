import matplotlib.pyplot as plt
import matplotlib.dates as mpdates
from mplfinance.original_flavor import candlestick_ohlc
import pandas as pd

pd.options.mode.chained_assignment = None

class Plotter:
    """
    This is the class aiming for plotting

    Attributes:
    titile(str): The string title of the plot
    """
    def __init__(self, title=''):
        self.title = title
    
    def plot(self, df, fig_name):
        plt.style.use('dark_background')
        #df['trade_date'] = pd.to_datetime(df['trade_date']).map(mpdates.date2num)
        
        # creating Subplots
        fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True)
        df['index'] = df.index
        # plotting the data
        candlestick_ohlc(
            ax[0], 
            df[['index', 'open', 'high', 'low', 'close']].values, 
            width = 0.6, 
            colorup = 'green', 
            colordown = 'red',
            alpha = 0.8)
        ax[0].grid(True)
        
        ax[0].plot(df['index'], df['ema_100'], '-', color='orange', alpha=0.8)
        ax[0].set_xticklabels([df.loc[i, 'trade_date'] for i in df.index], fontsize=4)
        
        # plot the volumn data as a bar chart
        ax[1].bar(df['index'], df['vol'], width=0.6, alpha=0.8)
        #ax[1].xaxis.set_major_formatter(mpdates.DateFormatter('%Y-%m-%d'))
        #fig.autofmt_xdate()
        ax[1].grid(True)
        

        # Setting labels
        ax[0].set_ylabel('Price')
        ax[1].set_xlabel('Date')
        ax[1].set_ylabel('Volume')

        plt.savefig('%s.png' % fig_name)
        print("The plot is succesfully saved.")
    
    def show_distribution(self, df, name):
        plt.style.use('dark_background')
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(df.index, df[name])
        ax.set_ylabel('Return')
        ax.set_xticklabels([df.loc[i, 'trade_date'] for i in df.index])
        plt.savefig('return_%s.png' % name)