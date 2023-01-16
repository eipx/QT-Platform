import pandas as pd
import EMAStrategy
import Pipeline
import tushare as ts
from credentials import *

class SignalGenerator:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def generate_signals(self, *strategies):
        for strategy in strategies:
            strategy.execute()

    def update_signals(self, *strategies):
        for strategy in strategies:
            strategy.update()

    

def main():
    ts.set_token(my_token)
    pro = ts.pro_api()
    
    pipeline = Pipeline.IndexPipeline('399300.SZ', '20170901', pro)
    if not pipeline.setup:
        pipeline.get_history()

    Strategy_ema100 = EMAStrategy.EMAStrategy(pipeline, 100)
    Strategy_ema50 = EMAStrategy.EMAStrategy(pipeline, 50)
    signalGenerator = SignalGenerator(pipeline)
    signalGenerator.generate_signals(Strategy_ema100, Strategy_ema50)
    signalGenerator.update_signals(Strategy_ema100, Strategy_ema50)

if __name__ == '__main__':
    main()


