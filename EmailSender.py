import smtplib
import pandas as pd
from credentials import user, password, to
from tqdm import tqdm

class EmailSender:
    
    def __init__(self, pipeline):
        self.pipeline = pipeline
    
    def send_update(self, alert_stock: list):
        subject = "Stock Daily Report"
        alert = "These stocks have crossovers between candlestick and EMA:\n"
        for i in range(len(alert_stock)):
            alert += (str(i) + " " + str(alert_stock[i]) + "\n")

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
        message = f'Subject: {subject}\n\n{headline}{alert}\n{body.to_string()}'
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(user, password)
            server.sendmail(user, to, message)

        print(f"Email sent to {to} with subject '{subject}'")