import tushare as ts
import pandas as pd
from credentials import *
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import create_engine
import traceback

ts.set_token(my_token)
pro = ts.pro_api()
my_conn = create_engine("mysql+pymysql://"+my_user+":"+my_password+"@localhost/qt_database")

class index_pipeline:
    def __init__(self, index_code, start_date, data_path):
        self.index_code = index_code
        self.start_date = start_date
        self.data_path = data_path

    def get_index_list(self):
        today = datetime.today().strftime("%Y%m%d")
        df = pro.index_weight(index_code=self.index_code, start_date=self.start_date, end_date=today)
        df = df[["con_date"]].rename({"con_code":"Index"}, axis=1)
        df.to_csv(self.data_path+'universe', dtype=str, index=False)

    def get_index_history_daily(self):
        # get the stock ID list
        stock_list = pd.read_csv(self.data_path+'universe', dtype=str)
        count = 0
        today = datetime.today().strftime("%Y%m%d")
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
                
        print("job_done! ")
    
    def update_hs300_daily(self):
        # read stock list file from universe
        today = datetime.today().strftime("%Y%m%d")
        stock_list = pd.read_csv(self.data_path+'universe', dtype=str)
        count = 0
        close_market_date = []
        for id in stock_list["Index"]:
            df = pd.read_csv(self.data_path+"daily_price/"+str(id)+".csv", dtype=str)  
            count += 1
            # if the dataframe is empty, assign the start date to most recent date
            if len(df) == 0:
                most_recent_date = datetime.strptime(self.start_date, "%Y%m%d")
            else:
                most_recent_date = datetime.strptime(df["trade_date"].iloc[0], "%Y%m%d")
            
            # check if all the data are up to date
            first_day = most_recent_date + timedelta(days=1)
            if  first_day not in close_market_date and first_day < datetime.today():
                lines_append = pro.daily(ts_code=id, start_date=first_day.strftime("%Y%m%d"), end_date=today)
                if len(lines_append) != 0:
                    lines_append = lines_append[["ts_code", "trade_date", "open", "close", "low", "high"]]
                    print(count, "updating id", id, "from", lines_append["trade_date"].iloc[-1], "to", today)
                    new_df = pd.concat([lines_append, df], ignore_index=True, axis=0)
                    new_df.to_csv(self.data_path+"daily_price/"+str(id)+'.csv', index=False)
                    continue
                else:
                    close_market_date.append(first_day)
            print(count, "id", id, "is already up to date")
        #print_stats()
            
        
if __name__ == "__main__":
    hs300_pipe = index_pipeline("399300.SZ", "20170901", "data/")
    hs300_pipe.get_index_history_daily()
    hs300_pipe.update_hs300_daily()
