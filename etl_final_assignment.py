import glob
import pandas as pd
from datetime import datetime

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

def extract():
    df = extract_from_json("bank_market_cap_1.json")
    df.columns=['Name','Market Cap (US$ Billion)']
    return df

#exchange_rates_df = pd.read_csv("exchange_rates.csv", index_col=0)
#exchange_rate = float(exchange_rates_df.loc['GBP'])

def transform(exchange_rate, extracted_df):
    extracted_df['Market Cap (US$ Billion)'] = round(extracted_df['Market Cap (US$ Billion)'] * exchange_rate, 3)
    extracted_df.columns = ['Name', 'Market Cap (GBP$ Billion)']
    return extracted_df

def load(transformed_df):
    transformed_df.to_csv("bank_market_cap_gbp.csv", index=False)

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a+") as f:
        f.write(timestamp + ',' + message + '\n')

if __name__ == '__main__':
    log("ETL Job Started")
    log("Extract phase Started")

    extracted_df = extract()
    print(extracted_df.head())

    log("Extract phase Ended")
    log("Transform phase Started")

    exchange_rates_df = pd.read_csv("exchange_rates.csv", index_col=0)
    exchange_rate = float(exchange_rates_df.loc['GBP'])

    transformed_df = transform(exchange_rate, extracted_df)
    print(transformed_df.head())

    log("Transform phase Ended")
    log("Load phase Started")

    load(transformed_df)

    log("Load phase Ended")