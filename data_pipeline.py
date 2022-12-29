import tushare as ts
import pandas as pd
from credentials import *
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import create_engine
from pytz import timezone
import traceback

market_timezone = timezone("Asia/Shanghai")
ts.set_token(my_token)
pro = ts.pro_api()
my_conn = create_engine("mysql+pymysql://"+my_user+":"+my_password+"@localhost/qt_database")

class IndexPipeline:

    def __init__(self, index_code, start_date, data_path, my_conn):
        self.index_code = index_code
        self.start_date = start_date
        self.data_path = data_path
        self.my_conn = my_conn

    def get_list(self):
        today = datetime.now(market_timezone).strftime("%Y%m%d")
        df = pro.index_weight(index_code=self.index_code, start_date=self.start_date, end_date=today)
        df = df[["con_date"]].rename({"con_code":"ts_code"}, axis=1)
        df.to_sql(con=my_conn, name="universe", if_exists="replace", index=False)

    def get_history(self):

        print("-----------------Getting history data, initializing pipeline-----------------")
        # empty the old history first
        my_conn.execute("TRUNCATE TABLE daily_prices")

        # get the stock ID list
        query = "SELECT ts_code FROM universe"
        stock_list = pd.read_sql(query, my_conn)
        count = 0
        today = datetime.now(market_timezone).strftime("%Y%m%d")
        for id in stock_list["ts_code"]:
            count += 1
            while True:
                # try to get daily price for each stock
                try:
                    df = pro.daily(ts_code=id, start_date=self.start_date, end_date=today)
                    df = df[["ts_code", "trade_date", "open", "close", "low", "high"]]
                    df.to_sql(con=my_conn, name="daily_prices", if_exists="append", index=False)
                    print(str(count) + " id: "+id+" successfully loaded")
                    break
                except Exception:
                    traceback.print_exc()
                    print("fail to get id: "+id+", repeat now...")
                    sleep(10)
                    continue
        print("job done")
    
    @property
    def setup(self):
        # get next date to update date
        query = "SELECT MAX(trade_date) AS date FROM daily_prices"
        today = datetime.now(market_timezone).date()
        today_str = today.strftime("%Y%m%d")
        df = pd.read_sql(query, my_conn)["date"]
        
        # if the Dataframe is empty, which means the database is empty, then it's not properly set up
        if df[0] == None:
            return False
        next_update_date = df[0] + timedelta(days=1)

        # check if the database is already up to date
        if next_update_date < today:
            duration = (today - next_update_date).days

            # just to bypass records per minute restriction, can be removed if the token is available
            if duration > 20:
                print("The date diff from last update date to today exceeds 20 days, please re-initialize the pipeline")
                return False
        return True

class DatabaseUpdater:

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def update_daily(self):
        # check if the database is properly set up first
        if not self.pipeline.setup:
            print("The pipeline is not properly set up, please set up pipeline before updating the data")
            return

        print("-----------------Updating database-----------------")
        # get next date to update date
        query = "SELECT MAX(trade_date) AS date FROM daily_prices"
        today = datetime.now(market_timezone).date()
        today_str = today.strftime("%Y%m%d")
        next_update_date = (pd.read_sql(query, self.pipeline.my_conn))["date"][0] + timedelta(days=1)
        next_update_str = next_update_date.strftime("%Y%m%d")
        
        # check if the database is already up to date
        if next_update_date < today:
            open_info = pro.query("trade_cal", start_date=next_update_str, end_date=today_str)
            if len(open_info) != 0 and (1 in open_info["is_open"]):
                query = "SELECT ts_code FROM universe"
                stock_list = pd.read_sql(query, self.pipeline.my_conn)["ts_code"]
                new_info = pro.daily(ts_code=','.join(stock_list.tolist()), start_date=next_update_str, end_date=today_str)
                new_info = new_info[["ts_code", "trade_date", "open", "close", "low", "high"]]
                new_info.to_sql(con=self.pipeline.my_conn, name="daily_prices", if_exists="append", index=False)
                print("The database is successfully updated!")
                return 
        print("The database is already up to date, no update are needed.")      

def main():
    pipeline = IndexPipeline("399300.SZ", "20170901", "data/", my_conn)
    if not pipeline.setup:
        pipeline.get_history()
    database_updater = DatabaseUpdater(pipeline)
    database_updater.update_daily()

if __name__ == "__main__":
    main()
