import tushare as ts
import pandas as pd
from credentials import *
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import create_engine, MetaData, Column, DECIMAL, Table
from pytz import timezone
import traceback
import smtplib
from tqdm import tqdm

market_timezone = timezone("Asia/Shanghai")
ts.set_token(my_token)
pro = ts.pro_api()
engine = create_engine("mysql+pymysql://"+my_user+":"+my_password+"@localhost/qt_database")

class IndexPipeline:

    def __init__(self, index_code, start_date, data_path, engine):
        self.index_code = index_code
        self.start_date = start_date
        self.data_path = data_path
        self.engine = engine

    def get_list(self):
        today = datetime.now(market_timezone).strftime("%Y%m%d")
        df = pro.index_weight(index_code=self.index_code, start_date=self.start_date, end_date=today)
        df = df[["con_date"]].rename({"con_code":"ts_code"}, axis=1)
        df.to_sql(con=engine, name="universe", if_exists="replace", index=False)

    def get_history(self):

        print("-----------------Getting history data, initializing pipeline-----------------")
        # empty the old history first
        engine.execute("TRUNCATE TABLE daily_prices")

        # get the stock ID list
        query = "SELECT ts_code FROM universe"
        stock_list = pd.read_sql(query, engine)
        count = 0
        today = datetime.now(market_timezone).strftime("%Y%m%d")
        for id in tqdm(stock_list["ts_code"], total=len(stock_list["ts_code"]), desc='Loading history data...'):
            count += 1
            while True:
                # try to get daily price for each stock
                try:
                    df = pro.daily(ts_code=id, start_date=self.start_date, end_date=today)
                    df = df[["ts_code", "trade_date", "open", "close", "low", "high"]]
                    df.to_sql(con=engine, name="daily_prices", if_exists="append", index=False)
                    #print(str(count) + " id: "+id+" successfully loaded")
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
        df = pd.read_sql(query, engine)["date"]
        
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
        next_update_date = (pd.read_sql(query, self.pipeline.engine))["date"][0] + timedelta(days=1)
        next_update_str = next_update_date.strftime("%Y%m%d")
        
        # check if the database is already up to date
        if next_update_date < today:
            open_info = pro.query("trade_cal", start_date=next_update_str, end_date=today_str)
            if len(open_info) != 0 and (1 in open_info["is_open"].values):
                query = "SELECT ts_code FROM universe"
                stock_list = pd.read_sql(query, self.pipeline.engine)["ts_code"]
                new_info = pro.daily(ts_code=','.join(stock_list.tolist()), start_date=next_update_str, end_date=today_str)
                new_info = new_info[["ts_code", "trade_date", "open", "close", "low", "high"]]
                new_info.to_sql(con=self.pipeline.engine, name="daily_prices", if_exists="append", index=False)
                print("The database is successfully updated!")
                return 
        print("The database is already up to date, no update are needed.")      


class EmailSender:
    
    def __init__(self, pipeline):
        self.pipeline = pipeline
    
    def send_update(self):
        subject = "Stock Daily Report"
        query = '''
                SELECT * 
                FROM ( 
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                    FROM daily_prices
                ) t
                WHERE rn <= 2;
                '''
        df = pd.read_sql(query, self.pipeline.engine)
        groups = df.groupby("ts_code")
        
        headline = "---------------------Daily Data Update---------------------\n\n"
        # Create the email body
        merged_rows = []
        for ts_code, group in tqdm(groups, total=len(groups), desc='Processing'):
            row = pd.Series({
                "ts_code" : ts_code, 
                "last_market_price" : "{:.2f}".format(group['close'].iloc[1]), 
                "curr_market_price" : "{:.2f}".format(group['close'].iloc[0]),
                "change" : round((group["close"].iloc[0]-group["close"].iloc[1])/group["close"].iloc[1]*100, 2)
            })
            merged_rows.append(row)
        body = pd.concat(merged_rows, axis=1).T

        # Send the email
        message = f'Subject: {subject}\n\n{headline}{body.to_string()}'
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(user, password)
            server.sendmail(user, to, message)

        print(f"Email sent to {to} with subject '{subject}'")


class EMACalculator:
    
    def __init__(self, pipeline, period):
        self.pipeline = pipeline
        self.period = period
        
    def calc_history(self):

        metadata = MetaData()
        my_table = Table('daily_prices', metadata, autoload=True, autoload_with=self.pipeline.engine)
        if not 'EMA%d'%self.period in my_table.columns:
            engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (
                'EMA%d'%self.period, 
                'ema_%d'%self.period, 
                'DECIMAL(10, 2)'))

        # Execute a SELECT query and store the results in a DataFrame
        query = 'SELECT ts_code, trade_date, close FROM daily_prices ORDER BY trade_date'
        df = pd.read_sql_query(query, self.pipeline.engine)
        # Calculate the EMA values
        groups = df.groupby('ts_code')
        merged_groups = []
        for ts_code, group in tqdm(
            groups, 
            total=len(groups), 
            desc='calulating EMA%d' % (self.period)):

            #print(len(group))
            # Calculate the EMA values
            group['ema_%d'%self.period] = round(group['close'].ewm(span=self.period).mean(), 2)
            merged_groups.append(group)
        new_table = pd.concat(merged_groups)
        new_table.drop('close', axis=1)

        print('Merging two tables...')
        merged_df = pd.merge(df, new_table, on=['ts_code', 'trade_date'])
        print('Writing to MySQL...')
        merged_df.to_sql(con=self.pipeline.engine, name="daily_prices", if_exists="replace", index=False)

def main():
    pipeline = IndexPipeline("399300.SZ", "20170901", "data/", engine)
    if not pipeline.setup:
        pipeline.get_history()
    database_updater = DatabaseUpdater(pipeline)
    database_updater.update_daily()
    #email_sender = EmailSender(pipeline)
    #email_sender.send_update()
    ema_calculator = EMACalculator(pipeline, 100)
    ema_calculator.calc_history()

if __name__ == "__main__":
    main()
