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

        try:
            self.pipeline.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (
                'daily_prices', 
                'ema_%d'%self.period, 
                'DECIMAL(10, 2)'))
        except Exception:
            pass
        # Execute a SELECT query and store the results in a DataFrame
        query = 'SELECT * FROM daily_prices ORDER BY trade_date'
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
