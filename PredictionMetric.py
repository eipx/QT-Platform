import pandas as pd
import numpy as np
import SignalGenerator
import Pipeline
import Plotter

class PredictionMetric:
    def __init__(self, signalGenerator):
        self.signalGenerator = signalGenerator
        self.plotter = Plotter.Plotter()
    
    def plot_return_distribution(self, stock, strategy, start_date, end_date):
        """
        This is the method use for plot the return distribution from a
        given date to end date

        Parameters:
        -------------
        stock: ts_code value of a specific stock
        strategy: the strategy used
        start_date: str
        end_date: str
        """
        df = self.signalGenerator.pipeline.get_stock(stock, start_date, end_date)
        buy_price = df['open'].iloc[0]
        df['return_%s' % strategy.name] = (df['close'] - buy_price) / buy_price * 100
        self.plotter.show_distribution(df, strategy.name)

    def generate_metric(self, strategy, start_date, end_date):
        """
        This is the method for generating the whole metric for a specific strategy
        from the start_date to end_date, we divide the daily data into different 
        groups based on the stock, and if the signal is positive, we consider it bought, 
        sell at the end_date

        Parameters:
        ------------
        strategy: the strategy used, e.g.EMAStrategy
        start_date: str
        end_date: str
        """
        metric = pd.DataFrame()
        df = self.signalGenerator.pipeline.get_all_stocks(start_date, end_date)
        for code, sub_df in df.groupby('ts_code'):
            sub_df['return'] = np.where(
                (sub_df['score_%s' % strategy.name] == 1) & (sub_df.index != len(df)-1), 
                (sub_df.iloc[-1]['close'] - sub_df['open'].shift(1)) / sub_df['open'].shift(1), 
                0)
            total_count = sub_df['score_%s' % strategy.name].sum()
            new_row = {
                'ts_code': code, 
                'signal_count': total_count,
                'win_rate': round(
                    (sub_df['return'] > 0).sum()*100/total_count if total_count != 0 else 0, 2),
                'major_drawback': round(sub_df['return'].min()*100, 2),
                'avg': round(sub_df.loc[sub_df['score_ema_100'] == 1, 'return'].mean()*100, 2)}
            new_row_df = pd.DataFrame([new_row], index = [0])
            metric = pd.concat([metric, new_row_df], ignore_index=True, axis=0)
        print(metric)
        metric.to_csv('Metric_%s.csv' % strategy.name)
