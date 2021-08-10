import requests
import pandas as pd
import datetime

BASE_URL = "https://graph.mirror.finance/graphql"

q1 = '''query { assets {token symbol}}'''
assets = requests.post(BASE_URL, json={"query":q1}).json()["data"]["assets"]

all_data = pd.DataFrame()
now = datetime.date
to_time = 1628563116000
for asset in assets:
	print(asset)
	from_time = 1609488000000
	while True:
		price_query = '''query {asset(token:"''' + asset["token"] + '''") {prices {history(interval: 1, from:''' +  str(from_time) + ''', to:''' +  str(to_time) + ''') {timestamp price}}}}'''
		data = requests.post(BASE_URL, json={"query":price_query}).json()["data"]["asset"]["prices"]["history"]
		for bar in data:
			all_data = all_data.append({"symbol":asset["symbol"], "time":bar["timestamp"], "price":bar["price"]}, ignore_index=True)
		last_stamp = max([bar["timestamp"] for bar in data])
		if last_stamp > to_time:
			all_data.to_csv("all_mirror_data.csv", index=False, header=True)
			break
		else:
			from_time = last_stamp
			print(from_time)


