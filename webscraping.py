from bs4 import BeautifulSoup
import requests
import pandas as pd

# gather web contents
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
html_data = requests.get(url).text
data = html_data[101:124]
print(data)

# scrape the data
soup = BeautifulSoup(html_data, "html.parser")

# create empty dataframe
data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])

for row in soup.find_all('tbody')[3].find_all('tr'):
    col = row.find_all('td')
    if (col != []):
        name = col[1].text
        market_cap = col[2].text
        data = data.append({"Name":name, "Market Cap (US$ Billion)":market_cap}, ignore_index=True)

df = pd.DataFrame(data)
print(df.head())

#loading the data to json file
data.to_json("bank_market_cap.json")