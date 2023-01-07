from credentials import my_token
import tushare as ts
import pytz
import time
from datetime import datetime, timedelta
import sched

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
        """
        This is the update job that needs to be executed daily.
        """
        print("Updating...")
        self.updater.update_daily()
        if self.calculator.update() == True:
            self.sender.send_update(self.calculator.find_crossover())

    def run(self):
        """
        This method will check set up the pipeline first, 
        then calculate the EMA history, give an example plot
        do job once and then schedule the job daily at 5pm in
        Shanghai time.
        """
        if not self.pipeline.setup:
            self.pipeline.get_history()
        self.calculator.calc_history()

        df = self.pipeline.get_stock('000001.SZ', start_date="2022-12-01", end_date="2023-01-04")
        self.plotter.plot(df)
        scheduler = sched.scheduler(time.time, time.sleep)

        #self.job()
        def schedule_task():
            now = datetime.now()
            now.replace(hour=20, minute=0, second=0, microsecond=0)
            scheduled_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
            if scheduled_time < now:
                scheduled_time += timedelta(days=1)
            scheduler.enterabs(time.mktime(scheduled_time.timetuple()), 1, self.job)
            scheduler.enterabs(time.mktime(scheduled_time.timetuple()), 1, schedule_task)
        
        schedule_task()
        scheduler.run()
        
        
        
        

        

            
        



    
