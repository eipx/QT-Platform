from datetime import datetime, timedelta
import pandas as pd


class DatabaseUpdater:
    """
    This class is responsible for the update job.

    Attributes:
    pipeline(IndexPipeline): an IndexPipeline object that connects to the database
    pro(tushare_api): store the tushare_api
    """
    def __init__(self, pipeline, tushare_api):
        self.pipeline = pipeline
        self.pro = tushare_api

    def update_daily(self):
        """
        Update the database on a daily basis, can also be used for updating a period of 
        time less than 20 days, otherwise need to initialize the database again. 
        """
        # check if the database is properly set up first
        if not self.pipeline.setup:
            print("The pipeline is not properly set up, please set up pipeline before updating the data")
            return

        print("-----------------Updating database-----------------")
        # get next date to update date
        query = "SELECT MAX(trade_date) AS date FROM daily_prices"
        today = datetime.now(self.pipeline.timezone).date()
        today_str = today.strftime("%Y%m%d")
        next_update_date = (pd.read_sql(query, self.pipeline.engine))["date"][0] + timedelta(days=1)
        next_update_str = next_update_date.strftime("%Y%m%d")
        
        # check if the database is already up to date
        if next_update_date < today:
            open_info = self.pro.query("trade_cal", start_date=next_update_str, end_date=today_str)
            if len(open_info) != 0 and (1 in open_info["is_open"].values):
                query = "SELECT ts_code FROM universe"
                stock_list = pd.read_sql(query, self.pipeline.engine)["ts_code"]
                new_info = self.pro.daily(ts_code=','.join(stock_list.tolist()), start_date=next_update_str, end_date=today_str)
                new_info = new_info[["ts_code", "trade_date", "open", "close", "low", "high"]]
                new_info.to_sql(con=self.pipeline.engine, name="daily_prices", if_exists="append", index=False)
                print("The database is successfully updated!")
                return 
        print("The database is already up to date, no update are needed.")      
