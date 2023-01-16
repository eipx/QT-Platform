from time import sleep
from pytz import timezone
from sqlalchemy import create_engine
from credentials import my_user, my_password
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

class IndexPipeline:
    """
    This class represents the data pipeline which connects the database and other utilities.

    Attributes:
    index_code(str): the code for this specific index.
    start_date(str): the earliest date of the record.
    engine(engine): the sqlalchemy engine that connects the database
    timezone(timezone) the local timezone of the A stock market
    pro(api): tushare_api
    """
    engine = create_engine("mysql+pymysql://"+my_user+":"+my_password+"@localhost/qt_database")
    timezone = timezone("Asia/Shanghai")

    def __init__(self, index_code, start_date, tushare_api):
        self.index_code = index_code
        self.start_date = start_date
        self.pro = tushare_api
        try:
            query = '''
            CREATE TABLE IF NOT EXISTS daily_prices(
                ts_code VARCHAR(16) NOT NULL,
                trade_date DATE NOT NULL,
                open DECIMAL(10, 2) NOT NULL,
                close DECIMAL(10, 2) NOT NULL,
                low DECIMAL(10, 2) NOT NULL,
                high DECIMAL(10, 2) NOT NULL,
                vol DECIMAL(32, 2) NOT NULL,
                amount DECIMAL(32, 3) NOT NULL,
                PRIMARY KEY (ts_code, trade_date)
            );
            '''
            self.engine.execute(query)
        except Exception as e:
            print(e)

    def get_list(self):
        """
        Get a list of all the stocks of this index from tushare API,
        and then write it to the database
        """
        today = datetime.now(self.timezone).strftime("%Y%m%d")
        df = self.pro.index_weight(index_code=self.index_code, start_date=self.start_date, end_date=today)
        df = df[["con_date"]].rename({"con_code":"ts_code"}, axis=1)
        df.to_sql(con=self.engine, name="universe", if_exists="replace", index=False)

    def get_history(self):
        """
        Get all the history data of the stocks based on the universe table from the start_date to today,
        then write it to the daily_prices table.
        """
        print("-----------------Getting history data, initializing pipeline-----------------")
        # empty the old history first
        self.engine.execute("TRUNCATE TABLE daily_prices")

        # get the stock ID list
        query = "SELECT ts_code FROM universe"
        stock_list = pd.read_sql(query, self.engine)
        count = 0
        today = datetime.now(self.timezone).strftime("%Y%m%d")
        for id in tqdm(stock_list["ts_code"], total=len(stock_list["ts_code"]), desc='Loading history data...'):
            count += 1
            while True:
                # try to get daily price for each stock
                try:
                    df = self.pro.daily(ts_code=id, start_date=self.start_date, end_date=today)
                    df = df[["ts_code", "trade_date", "open", "close", "low", "high", "vol", "amount"]]
                    df.to_sql(con=self.engine, name="daily_prices", if_exists="append", index=False)
                    #print(str(count) + " id: "+id+" successfully loaded")
                    break
                except Exception as e:
                    print(e)
                    print("fail to get id: "+id+", repeat now...")
                    sleep(10)
                    continue
        print("job done")

    def get_all_stocks(self, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Parameters:
        ts_code(str): the stock code.
        start_date: the dataframe start date.
        end_date: the dataframe end date.
        
        Returns:
        the dataframe of the daily prices with the given parameters.
        """
        if start_date is None:
            query = 'SELECT * FROM daily_prices'
        else:
            query = '''
                SELECT * 
                FROM daily_prices 
                WHERE trade_date BETWEEN "%s" AND "%s" ;
                '''% (start_date, end_date)
        df = pd.read_sql(query, self.engine)
        return df

    def get_stock(self, ts_code: str, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Parameters:
        ts_code(str): the stock code.
        start_date: the dataframe start date.
        end_date: the dataframe end date.
        
        Returns:
        the dataframe of the daily prices with the given parameters.
        """
        if start_date is None:
            query = 'SELECT * FROM daily_prices WHERE ts_code = "%s"' % ts_code
        else:
            query = '''
                SELECT * 
                FROM daily_prices 
                WHERE ts_code = "%s" AND trade_date BETWEEN "%s" AND "%s" ;
                '''% (ts_code, start_date, end_date)
        df = pd.read_sql(query, self.engine)
        return df

    @property
    def setup(self):
        """
        Check if the pipeline is properly set up by comparing the latest date in the database
        and today's date. If the date difference is larger that 20, then it's not properly set 
        up. Same applies if the database is empty.
        """
        # get next date to update date
        query = "SELECT MAX(trade_date) AS date FROM daily_prices;"
        today = datetime.now(self.timezone).date()
        df = pd.read_sql(query, self.engine)["date"]
        
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


if __name__ == '__main__':
    pass