from credentials import my_token
import tushare as ts
import pytz
import schedule
import time

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

    def job(self):
        self.updater.update_daily()
        if self.calculator.update() == True:
            self.sender.send_update(self.calculator.find_crossover())

    def run(self):
        if not self.pipeline.setup:
            self.pipeline.get_history()
        time_zone = pytz.timezone("Asia/Shanghai")
        self.calculator.calc_history()

        df = self.pipeline.get_stock('000001.SZ')
        self.plotter.plot(df)

        schedule.every().day.at("17:00", timezone=time_zone).do(self.job)
        
        self.job()

        while True:
            # check for scheduled jobs
            schedule.run_pending()
            time.sleep(1)
        
        
        

        

            
        



    
