import json

import requests
import pandas as pd
from pandas import json_normalize

url = "http://api.exchangeratesapi.io/v1/latest?access_key=87080aa2710f5b6f98b3386c79712e62"
data = requests.get(url).text

exchange_rate = pd.read_json(data)
exchange_rate = exchange_rate["rates"]
#print(exchange_rate)

exchange_rate.to_csv("exchange_rates_1.csv")