import requests
import pandas as pd
import datetime
import schedule
import time

QUERY = '''query { assets {name symbol prices{price, oraclePrice} statistic{liquidity shortValue volume apr{long short} marketCap collateralValue minCollateralRatio}}}'''
BASE_URL = "https://graph.mirror.finance/graphql"

def req_data():

	now = int(datetime.datetime.today().timestamp())
	data_df = pd.DataFrame()
	data = requests.post(BASE_URL, json={"query":QUERY}).json()["data"]["assets"]
	for asset in data:
		try:
			data_df = data_df.append({"name":asset["name"], "symbol":asset["symbol"],"timestamp":now,"price":asset["prices"]["price"], "oraclePrice":asset["prices"]["oraclePrice"], "liquidity":asset["statistic"]["liquidity"], "shortValue":asset["statistic"]["shortValue"], "volume":asset["statistic"]["volume"], "apr_long":asset["statistic"]["apr"]["long"], "apr_short":asset["statistic"]["apr"]["short"], "marketCap":asset["statistic"]["marketCap"], "collateralValue":asset["statistic"]["collateralValue"], "minCollateralRatio":asset["statistic"]["minCollateralRatio"]}, ignore_index=True)
		except:
			pass
	data_df.to_csv("mirror_data.csv", header=False, index=False, mode="a")
	print(data_df)


schedule.every().minute.at(":05").do(req_data)
schedule.every().minute.at(":35").do(req_data)

while True:
	schedule.run_pending()
	time.sleep(1)