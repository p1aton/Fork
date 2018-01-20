import time
import sys
import websocket
import psycopg2
import psycopg2.extras
#import config
#conn_string = config.conn_string
import requests
from forex_python.converter import CurrencyRates 
c = CurrencyRates()


while True:
    start = time.time()
    try:
        
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO wex_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('wex_ts_id_seq');")
        bithumb_id = cur.fetchone() #! may be another title
        conn.commit()
        cur.close()
        conn.close()
        
        bithumb_crypts = ['BTC', 'ETH', 'DASH', 'LTC', 'ETC', 'XRP', 'BCH', 'XMR', 'ZEC', 'BTG', 'EOS']
        bithumb_nums = ['btc','eth','dsh','ltc','etc','xrp','bch','xmr','zec']

      
        bithumb_out = {'krw':1000, 'btc': 0.003, 'eth':0.01, 'dsh': 0.01, 'ltc':0.01, 'etc': 0.01, 'xrp': 1, 'bch':0.005, 'xmr':0.05, 'zec':0.001}
        
        bithumb_data = requests.get('https://api.bithumb.com/public/ticker/ALL', timeout = 2).json()
        
        com = c.get_rates('USD')['KRW']
        bithumb_values = []
        bithumb = []
        for i in bithumb_crypts:
            bithumb_values.append(float(bithumb_data['data'][i]['buy_price'])/com)
        
        for i in range(0,len(bithumb_data)):
            bithumb.append( (bithumb_nums,bithumb_values,bithumb_id[0]) )
        bithumb_answer = 1
    except:
        
        bithumb_answer = 0
    finally:
        
        # пишем в базу
        
        time.sleep(10 - (time.time() - start))
     