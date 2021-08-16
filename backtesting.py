import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os

def get_df(symbol):

	s_df = pd.read_csv("data/" + symbol + ".csv").drop_duplicates()
	s_df = s_df.rename(columns={"timestamp":"time"})
	s_df["time"] = pd.to_datetime(s_df["time"])
	s_df = s_df.set_index("time").asfreq("min")
	s_df["close"] = s_df["close"].fillna(method="ffill")
	for x in ["open","high","low"]:
		s_df[x] = s_df[x].fillna(s_df["close"])
	for x in ["volume","trade_count"]:
		s_df[x] = s_df[x].fillna(0)
	s_df = s_df.reset_index()
	s_df["time"] = s_df["time"].apply(lambda x: int(x.timestamp()))

	m_df = pd.read_csv("data/m" + symbol + ".csv").drop_duplicates()
	m_df["time"] = m_df["time"].apply(lambda x: int(str(x)[:-3]))

	df = s_df.merge(m_df, on=["time"])
	return df


def visualize_spread(symbol):

	df = get_df(symbol)
	df["spread"] = df["close"] - df["price"]
	df["datetime"] = df["time"].apply(lambda x: datetime.datetime.fromtimestamp(x))
	df["mavg"] = df["spread"].rolling(2880).mean()
	std = df["spread"].rolling(2880).std()
	df["upper_band"] = df["mavg"] + std
	df["lower_band"] = df["mavg"] - std
	plt.plot(df["datetime"], df[["spread","mavg","upper_band","lower_band"]])
	plt.show()


def backtest(stds, window, target):

	stocks = []
	for file in os.listdir("data"):
		if "m" not in file:
			stocks.append(file.replace(".csv", ""))

	final_cash = {x : None for x in stocks}
	data = []
	for s in stocks:
		try:
			df = get_df(s)
			data.append((s, df))
		except:
			continue
	for symbol, df in data:
		print(symbol)
		df["datetime"] = df["time"].apply(lambda x: datetime.datetime.fromtimestamp(x))
		df["spread"] = df["close"] - df["price"]
		df["mavg"] = df["spread"].rolling(window).mean()
		std = df["spread"].rolling(window).std()
		df["upper_band"] = df["mavg"] + std * stds
		df["lower_band"] = df["mavg"] + std * stds
		df = df[["time","datetime","price","oracle_price","close","spread","mavg","upper_band","lower_band"]]
		df = df.dropna(how="any")
		total_cash_used = 100000
		cash = 100000
		trade_cash_used = 0
		fees_paid = 0
		qty = 0
		collateral = 0
		for ind, row in df.iterrows():

			if row["spread"] > row["upper_band"] and qty == 0 and (row["datetime"].hour in range(15, 21) or (row["datetime"].hour == 14 and row["datetime"].minute >= 30)):
				#mint and enter short position
				qty = cash / (row["oracle_price"] * -2)
				fees_paid += qty * row["oracle_price"] * -0.003
				collateral = cash
				cash = qty * row["price"]
				fees_paid += qty + row["price"] * -0.003

			if row["spread"] < row["lower_band"] and qty == 0:
				#buy long position
				qty = cash / row["price"]
				trade_cash_used = cash
				fees_paid += qty * row["price"] * 0.003
				cash = 0

			if qty > 0 and row["spread"] > row["lower_band"]:
				exit_fee = qty * row["price"] * 0.003
				if ((row["price"] * qty - trade_cash_used - fees_paid - exit_fee) / (trade_cash_used + fees_paid + exit_fee)) > target:
					cash = row["price"] * qty - fees_paid - exit_fee
					total_cash_used += fees_paid + exit_fee
					fees_paid = 0
					qty = 0

			if qty < 0 and row["spread"] < row["upper_band"]:
				exit_fee = qty * row["price"] * -0.018
				if ((cash + qty * row["price"] - fees_paid - exit_fee) / (collateral + fees_paid + exit_fee)) > target:
					cash = collateral + cash + qty * row["price"] - fees_paid - exit_fee
					total_cash_used += fees_paid + exit_fee
					qty = 0
					fees_paid = 0

		if qty > 0:
			cash = trade_cash_used
		elif qty < 0:
			cash = collateral
		final_cash[symbol] = cash

	return final_cash




