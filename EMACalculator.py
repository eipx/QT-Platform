import pandas as pd
from tqdm import tqdm

class EMACalculator:
    """
    This class is responsible for calculating the EMA value for the table

    Attributes:
    pipeline(IndexPipeline): an IndexPipeline object to connect to the database
    period(int): the calculating period of the EMA
    """
    def __init__(self, pipeline, period):
        self.pipeline = pipeline
        self.period = period
        
    def calc_history(self):
        """
        This method is used for calculating all the EWA history
        """
        try:
            self.pipeline.engine.execute('ALTER TABLE %s ADD COLUMN %s %s;' % (
                'daily_prices', 
                'ema_%d'%self.period, 
                'DECIMAL(10, 2)'))
        except Exception:
            pass
        # Execute a SELECT query and store the results in a DataFrame
        query = 'SELECT * FROM daily_prices ORDER BY trade_date;'
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
        calc_result = pd.concat(merged_groups)
        print('Inserting the calculated result to the database...')
        calc_result.to_sql(con=self.pipeline.engine, name="daily_prices", if_exists="replace", index=False)

    def update(self) -> bool:
        """
        This method is used for updating data, which create an temporary table intermediate
        and then update the daily_prices table.
        """
        query = """
                SELECT * 
                FROM daily_prices
                WHERE ema_%d IS NULL
                ORDER BY trade_date;
                """ % self.period
        df_to_calc = pd.read_sql_query(query, self.pipeline.engine)
        if df_to_calc.empty:
            print("All the EMA%d values have already been calculated." % self.period)
            return False

        latest_date = df_to_calc['trade_date'].min()

        query = """
                SELECT MAX(trade_date) 
                AS date
                FROM daily_prices
                WHERE trade_date < "%s";
                """ % latest_date
        df_prior = pd.read_sql_query(query, self.pipeline.engine)
        if df_prior['date'].iloc[0] is None:
            print("Please calculate the EMA%d history first" % self.period)
            return False

        query = """
                SELECT *
                FROM daily_prices
                WHERE trade_date = "%s";
                """ % df_prior['date'].iloc[0]
        df_addon = pd.read_sql_query(query, self.pipeline.engine)
        df = pd.concat([df_addon, df_to_calc])[[
            "ts_code", 
            "trade_date", 
            "close", 
            "ema_%d"%self.period]]
        groups = df.groupby('ts_code')
        merged_groups = []
        for ts_code, group in tqdm(
            groups, 
            total=len(groups), 
            desc='updating EMA%d' % (self.period)):

            #print(len(group))
            # Calculate the EMA values
            group['ema_%d'%self.period] = round(group['close'].shift(1).ewm(span=self.period).mean(), 2)
            merged_groups.append(group[1:])
        calc_result = pd.concat(merged_groups)
        print('Updating the new EMA%s result to the database...' % self.period)
        query = """
        CREATE TABLE IF NOT EXISTS intermediate(
            ts_code VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            close DECIMAL(10, 2) NOT NULL,
            ema_%d Decimal(10, 2) NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
            );""" % self.period
        self.pipeline.engine.execute(query)
        calc_result.to_sql(
            con=self.pipeline.engine, 
            name="intermediate", 
            if_exists="append", 
            index=False)
        try:
            self.pipeline.engine.execute("""
                UPDATE daily_prices
                JOIN intermediate
                ON daily_prices.ts_code = intermediate.ts_code 
                AND daily_prices.trade_date = intermediate.trade_date
                SET daily_prices.ema_%d = intermediate.ema_%d;
                """ %(self.period, self.period)
            )
        except Exception as e:
            print(e)
        finally:
            self.pipeline.engine.execute("DROP TABLE intermediate;")
            return True
    
    def find_crossover(self) -> list:
        """
        This method is used to identify stocks that 
        have a crossover between the candlestick and the EMA.
        """
        stocks = []
        query = """
        SELECT *
        FROM daily_prices
        WHERE trade_date IN (
            SELECT MAX(trade_date) 
            FROM daily_prices
            )
        AND ema_%d BETWEEN open AND close;
        """ % self.period
        df = pd.read_sql_query(query, self.pipeline.engine)
        return df["ts_code"].tolist()

