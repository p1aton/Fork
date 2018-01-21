import time
import sys
import websocket
import psycopg2
import psycopg2.extras
from btfxwss import BtfxWss
import queue
import config
conn_string = config.conn_string
q = queue.Queue() 

bitfinex_pairs = ["BTCUSD","LTCUSD","LTCBTC","ETHUSD","ETHBTC","ETCBTC","ETCUSD","ZECUSD","ZECBTC",
         "XMRUSD","XMRBTC","DSHUSD","DSHBTC","BTCEUR","XRPUSD","XRPBTC","BCHUSD","BCHBTC","BCHETH"]

bitfinex_nums = ['btc_usd','ltc_usd','ltc_btc','eth_usd','eth_btc','etc_btc','etc_usd','zec_usd','zec_btc',
            'xmr_usd','xmr_btc','dsh_usd','dsh_btc','btc_eur','xrp_usd','xrp_btc','bch_usd','bch_btc','bch_eth']

wss = BtfxWss()
wss.start()
while not wss.conn.connected.is_set():
    time.sleep(1)
# Subscribe to some channels
for i in bitfinex_pairs:
    wss.subscribe_to_ticker(i)
time.sleep(10)

bitfinex_values = []

while True:
    start = time.time()

    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO btfnx_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('btfnx_ts_id_seq');")
        btfnx_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close() 
        for i in bitfinex_pairs:
            bitfinex_values.append(wss.tickers(i).get(block=False)[0][0][6])
        
        bitfinex = []
		bitfinex_answer = 1
        for i in range(0,len(bitfinex_nums)):
            bitfinex.append((bitfinex_nums[i],bitfinex_values[i], btfnx_id[0],bitfinex_answer,))
        

    except queue.Empty: 
    
        bitfinex_answer = 0
    
    except Exception: 
        bitfinex_answer = 0
    
    finally:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO btfnx(br, value, idt) values %s", bitfinex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10 - (time.time() - start))

wss.stop()
