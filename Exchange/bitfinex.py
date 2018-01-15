import logging
import time
import sys
import websocket
import psycopg2
import psycopg2.extras
from btfxwss import BtfxWss
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
log = logging.getLogger(__name__)

fh = logging.FileHandler('test.log')
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.DEBUG)

log.addHandler(sh)
log.addHandler(fh)
logging.basicConfig(level=logging.DEBUG, handlers=[fh, sh])


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
        for i in bitfinex_pairs:
            bitfinex_values.append(wss.tickers(i).get()[0][0][6])
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO btfnx_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('btfnx_ts_id_seq');")
        btfnx_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        bitfinex = []
        for i in range(0,len(bitfinex_nums)):
            bitfinex.append((bitfinex_nums[i],bitfinex_values[i], btfnx_id[0],))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO btfnx(br, value, idt) values %s", bitfinex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10 - time.time() - start)

    except : 
        wss = BtfxWss()
        wss.start()
        while not wss.conn.connected.is_set():
            time.sleep(1)
        # Subscribe to some channels
        for i in bitfinex_pairs:
            wss.subscribe_to_ticker(i)
        time.sleep(10)

        bitfinex_values = []
        for i in bitfinex_pairs:
            bitfinex_values.append(wss.tickers(i).get()[0][0][6])
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO btfnx_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('btfnx_ts_id_seq');")
        btfnx_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        bitfinex = []
        for i in range(0,len(bitfinex_nums)):
            bitfinex.append((bitfinex_nums[i],bitfinex_values[i], btfnx_id[0],))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO btfnx(br, value, idt) values %s", bitfinex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(21 - (time.time() - start))
wss.stop()