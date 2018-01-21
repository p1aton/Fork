import binance
from binance.client import Client
import time
import config
import psycopg2
import psycopg2.extras
conn_string = config.conn_string
client = Client('dtiWWNHXnLzD2RNe2QivRsRnrUoKWZksOgl9MwZEGuTFdyr3X2hnLZgzC5OgFp93', 
               'LjprvlFaGf6rc42eAynezgApSbEcCYz9YGWU3EsLO9bmMxJkhDc41YrLodjnzfPR')
binance_pairs = ['ETHBTC','LTCBTC','NEOBTC','BTCUSDT','ETHUSDT','ZECBTC','ZECETH','ETCETH','ETCBTC','DASHBTC','DASHETH','XRPBTC',
                'XRPETH','XMRBTC','XMRETH','LTCETH','LTCUSDT']
binance_nums = ['eth_btc','ltc_btc','neo_btc','btc_usd','eth_usd','zec_btc','zec_eth','etc_eth','etc_btc','dsh_btc','dsh_eth',
                'xrp_btc','xrp_eth','xmr_btc','xmr_eth','ltc_eth','ltc_usd']

while True:
    start = time.time()
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO binance_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('binance_ts_id_seq');")
        binance_id = cur.fetchone()
        cur.execute("SELECT value from usdt order by id desc limit 1;")
        usdt_usd = float(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()
        prices = client.get_all_tickers()
        binance = []
        binance_values = []
        binance_p = []
        binance_s = []
        binance_right_p = []
        for i in range(0,len(prices)):
                binance_p.append(prices[i]['price'])
                binance_s.append(prices[i]['symbol'])
        binance_i = dict(zip(binance_s,binance_p))

        for i in range(0,len(binance_pairs)):
            if binance_pairs[i][3:6] == 'USD':
                binance_values.append(float(binance_i[binance_pairs[i]])*1.01)
            else:
                binance_values.append(binance_i[binance_pairs[i]])
        
		binance_answer = 1
        
      
        
        
    except Exception:
        binance_answer = 0
    
    finally:
		for i in range(0,len(binance_nums)):
            binance.append((binance_nums[i], binance_values[i], binance_id[0],binance_answer))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO binance(br, value, idt) values %s", binance)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10 - (time.time() - start))
