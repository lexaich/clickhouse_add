from clickhouse_driver import Client
import requests

client = Client('localhost')
# 200000 записей
url_str = "http://min-api.cryptocompare.com/data/histoday?fsym=SMT*&tsym=BTC&limit="
nums = "200000"

url = '{0}{1}'.format(url_str, nums)
data = requests.get(url).json()["Data"]

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
# client.execute('DROP TABLE IF EXISTS blocks')
client.execute('CREATE TABLE IF NOT EXISTS blocks ({}) ENGINE = MergeTree() ORDER BY (time)'.format(schema_create))
schema_insert = ",".join(schema.keys())
client.execute('INSERT INTO blocks ({}) VALUES'.format(schema_insert), data)