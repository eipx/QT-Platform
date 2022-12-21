import tushare as ts
import pandas as pd
import os
from ts_token import my_token
from datetime import datetime, timedelta
from time import sleep
import traceback

ts.set_token(my_token)
pro = ts.pro_api()

start_date = "20210901"
today = datetime.today().strftime("%Y%m%d")
data_path = "data/"

def get_hs300_history_daily(start_date=start_date, end_date=today):
    stock_list = pd.read_csv(data_path+'universe', dtype=str)
    count = 0
    os.makedirs(data_path+"daily_price", exist_ok=True)
    for id in stock_list["Index"]:
        count += 1
        while True:
            try:
                df = pro.daily(ts_code=id, start_date=start_date, end_date=end_date)
                df = df[["ts_code", "trade_date", "open", "close", "low", "high"]]
                df.to_csv(data_path+"daily_price/"+str(id)+'.csv', index=False)
                print(str(count) + " id: "+id+" successfully loaded")
                break
            except Exception:
                traceback.print_exc()
                print("fail to get id: "+id+", repeat now...")
                sleep(10)
                continue
            
    print("job_done! ")

def update_hs300_daily():
    stock_list = pd.read_csv(data_path+'universe', dtype=str)
    count = 0
    for id in stock_list["Index"]:
        df = pd.read_csv(data_path+"daily_price/"+str(id)+".csv", dtype=str)  
        count += 1
        if len(df) == 0:
            most_recent_date = datetime.strptime(start_date, "%Y%m%d")
        else:
            most_recent_date = datetime.strptime(df["trade_date"].iloc[0], "%Y%m%d")

        if most_recent_date < datetime.today():
            next_date = most_recent_date + timedelta(days=1) if most_recent_date is not None else start_date
            lines_append = pro.daily(ts_code=id, start_date=next_date.strftime("%Y%m%d"), end_date=today)
            if len(lines_append) != 0:
                lines_append = lines_append[["ts_code", "trade_date", "open", "close", "low", "high"]]
                print(count, "updating id", id, "from", lines_append["trade_date"].iloc[-1], "to", today)
                new_df = pd.concat([lines_append, df], ignore_index=True, axis=0)
                new_df.to_csv(data_path+"daily_price/"+str(id)+'.csv', index=False)
                continue
        print(count, "id", id, "is already up to date")
            
        
if __name__ == "__main__":
    #get_hs300_history_daily(end_date="20211001")
    update_hs300_daily()
