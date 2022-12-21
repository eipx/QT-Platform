import tushare as ts
import pandas as pd
from ts_token import my_token
import datetime
from time import sleep
import traceback

ts.set_token(my_token)
pro = ts.pro_api()

start_date = "20170901"
end_date = datetime.date.today().strftime("%Y%m%d")
data_path = "data/"

def get_hs300_history_daily(start_date=start_date, end_date=end_date):
    stock_list = pd.read_csv(data_path+'universe', dtype=str)
    count = 0
    for id in stock_list["Index"]:
        while True:
            try:
                df = pro.daily(ts_code=id, start_date=start_date, end_date=end_date)
                df = df[["ts_code", "trade_date", "open", "close", "low", "high"]]
                df.to_csv(data_path+id+'.csv', index=False)
                count += 1
                print(str(count) + " id: "+id+" successfully loaded")
                break
            except Exception:
                traceback.print_exc()
                print("fail to get id: "+id+", repeat now...")
                sleep(10)
                continue
            
    print("job_done! ")
	

if __name__ == "__main__":
	get_hs300_history_daily()
