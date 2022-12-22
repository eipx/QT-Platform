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

class index_pipeline:
    def __init__(self, index_code, start_date, data_path):
        self.index_code = index_code
        self.start_date = start_date
        self.data_path = data_path

    def get_index_list(self):
        today = datetime.now(market_timezone).strftime("%Y%m%d")
        df = pro.index_weight(index_code=self.index_code, start_date=self.start_date, end_date=today)
        df = df[["con_date"]].rename({"con_code":"ts_code"}, axis=1)
        df.to_csv(self.data_path+'universe', dtype=str, index=False)

    def get_index_history_daily(self):
        # empty the old history first
        my_conn.execute("TRUNCATE TABLE daily_prices")

        # get the stock ID list
        stock_list = pd.read_csv(self.data_path+'universe', dtype=str)
        count = 0
        today = datetime.now(market_timezone).strftime("%Y%m%d")
        for id in stock_list["Index"]:
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
        self.setup = True  
        print("job done!")
    
    def update_hs300_daily(self):
        # get next date to update date
        query = "SELECT MAX(trade_date) AS date FROM daily_prices"
        today = datetime.now(market_timezone).date()
        today_str = today.strftime("%Y%m%d")
        next_update_date = (pd.read_sql(query, my_conn))["date"][0] + timedelta(days=1)
        next_update_str = next_update_date.strftime("%Y%m%d")
        
        # check if the database is already up to date
        if next_update_date < today:
            duration = (today - next_update_date).days

            # just to bypass records per minute restriction, can be removed if the token is available
            if duration > 20:
                print("The date diff from last update date to today exceeds 20 days, please re-initialize the history data")
                return
            open_info = pro.query("trade_cal", start_date=next_update_str, end_date=today_str)
            stock_list = ""
            if len(open_info) != 0 and (1 in open_info["is_open"]):
                query = "SELECT ts_code FROM universe"
                stock_list_df = pd.read_sql(query, my_conn)
                for i in range(len(stock_list_df)):
                    stock_list += stock_list_df["ts_code"].iloc[i] + ","
                new_info = pro.daily(ts_code=stock_list[:-1], start_date=next_update_str, end_date=today_str)
                new_info = new_info[["ts_code", "trade_date", "open", "close", "low", "high"]]
                new_info.to_sql(con=my_conn, name="daily_prices", if_exists="append", index=False)
                print("Database successfully updated!")
                return
        print("Database is already up to date!")
            
        
if __name__ == "__main__":
    hs300_pipe = index_pipeline("399300.SZ", "20170901", "data/")
    hs300_pipe.get_index_history_daily()
    hs300_pipe.update_hs300_daily()
