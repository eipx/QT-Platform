from credentials import my_token
import tushare as ts
import pytz

import Pipeline
import DatabaseUpdater
import EMACalculator
import EmailSender
import Plotter


class TaskScheduler:
    """
    This is the core class that handles the automation
    it will update the database every day at 5 pm.
    """
    ts.set_token(my_token)
    pro = ts.pro_api()
    
    def __init__(self, index_code, start_date):
        self.pipeline = Pipeline.IndexPipeline(index_code, start_date, self.pro)
        self.updater = DatabaseUpdater.DatabaseUpdater(self.pipeline, self.pro)
        self.sender = EmailSender.EmailSender(self.pipeline)
        self.calculator = EMACalculator.EMACalculator(self.pipeline, 100)
        self.plotter = Plotter.Plotter(self.pipeline)

    def run(self):
        if not self.pipeline.setup:
            self.pipeline.get_history()
        time_zone = pytz.timezone("Asia/Shanghai")

        self.calculator.calc_history()
        self.updater.update_daily()
        self.calculator.calc_history()
        df = self.pipeline.get_stock('000001.SZ')
        self.plotter.plot(df)

        

            
        



    
