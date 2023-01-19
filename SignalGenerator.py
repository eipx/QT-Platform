import pandas as pd
import EMAStrategy
import Pipeline
import tushare as ts
from credentials import *
import PredictionMetric

class SignalGenerator:
    """
    The SignalGenerator class is responsible for managing the 
    existing strategies, generate the signals for all the strategies
    in the list.

    Parameters for init
    --------
    pipeline: Indexpipeline object for database connection
    strategies: The strategy list.
    """
    def __init__(self, pipeline, *strategies):
        self.pipeline = pipeline
        self.strategies = list(strategies)

    def add_strategies(self, *strategies):
        self.strategies.extend(strategies)
        print(strategies, " added!")

    def generate_signals(self):
        for strategy in self.strategies:
            strategy.execute()

    def update_signals(self):
        for strategy in self.strategies:
            strategy.update()

    def fetch_signals(self, start_date, end_date) -> pd.DataFrame:

        merged_df = self.strategies[0].fetch_signals(start_date, end_date)
        for strategy in self.strategies[1:]:
            df = strategy.fetch_signals(start_date, end_date)
            merged_df = pd.merge(merged_df, df, on=['trade_date','ts_code'])
            
        return merged_df

def main():
    ts.set_token(my_token)
    pro = ts.pro_api()
    
    pipeline = Pipeline.IndexPipeline('399300.SZ', '20170901', pro)
    if not pipeline.setup:
        pipeline.get_history()

    Strategy_ema100 = EMAStrategy.EMAStrategy(pipeline, 100)
    Strategy_ema50 = EMAStrategy.EMAStrategy(pipeline, 50)
    signalGenerator = SignalGenerator(pipeline)
    signalGenerator.add_strategies(Strategy_ema100, Strategy_ema50)

    signalGenerator.generate_signals()
    signalGenerator.update_signals()

    metric = PredictionMetric.PredictionMetric(signalGenerator)

    metric.plot_return_distribution('000001.SZ', Strategy_ema100, "2020-08-04", "2023-01-11")
    metric.generate_metric(Strategy_ema100, "2020-08-04", "2023-01-11")
if __name__ == '__main__':
    main()


