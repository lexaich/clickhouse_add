# -*- coding: utf-8 -*-
from clickhouse_driver import Client
import requests

client = Client('localhost')

url_coins = "https://www.cryptocompare.com/api/data/coinlist/"
url_str_frst = "https://min-api.cryptocompare.com/data/histoday?fsym="
url_str_scnd = "*&tsym=BTC&limit=1000"
schema = {
	"open": "Float32",
	"volumeto": "Float32",
	"volumefrom": "Float32",
	"high": "Float32",
	"time": "DateTime",
	"low": "Float32",
	"close": "Float32"
}
schema_create = ", ".join(["{} {}".format(a, b) for a, b in schema.items()])
schema_insert = ",".join(schema.keys())
coins = requests.get(url_coins).json()["Data"]

for coin in coins:
	url = '{0}{1}{2}'.format(url_str_frst,coin,url_str_scnd)
	req = requests.get(url)
	if(req.status_code==200):
		data = req.json()["Data"]
		if data:
			print(coin)
			try:
				client.execute('DROP TABLE IF EXISTS {0}_blocks'.format(coin))
				client.execute('CREATE TABLE IF NOT EXISTS {0}_blocks ({1}) ENGINE = MergeTree() ORDER BY (time)'.format(coin,schema_create))
				client.execute('INSERT INTO {0}_blocks ({1}) VALUES'.format(coin,schema_insert), data)
			except:
				print('exept symbol')
			