from poloniex import Poloniex
import psycopg2
import psycopg2.extras
import time
import config
conn_string = config.conn_string
polo = Poloniex()

poloniex_tickers = ['BTC_DASH','BTC_LTC','BTC_XRP','BTC_ETH','USDT_ETH','USDT_BTC','USDT_DASH','USDT_LTC','USDT_XRP','USDT_XMR',
            'XMR_LTC','XMR_DASH','BTC_ETC','ETH_ETC','USDT_ETC','BTC_ZEC','ETH_ZEC','USDT_ZEC','BTC_BCH','ETH_BCH','USDT_BCH']

poloniex_words = ['dsh_btc','ltc_btc','xrp_btc','eth_btc','eth_usd','btc_usd','dsh_usd','ltc_usd','xrp_usd','xmr_usd',
                 'ltc_xmr','dsh_xmr','etc_btc','etc_eth','etc_usd','zec_btc','zec_eth','zec_usd','bch_btc','bch_eth','bch_usd']
while True:
    start = time.time()
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO pol_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('pol_ts_id_seq');")
        pol_id = cur.fetchone()
        cur.execute("SELECT value from usdt order by id desc limit 1;")
        usdt_usd = float(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()                
        poloniex_data = polo.returnTicker() 
        poloniex_last_price = []
        
        
        for i in poloniex_tickers:
            poloniex_last_price.append(poloniex_data[i]['last'])        
        poloniex_answer = 1
        poloniex = []
        for i in range(0,len(poloniex_words)):
            if poloniex_tickers[i][0:4] == 'USDT':
                poloniex.append((poloniex_words[i],poloniex_last_price[i]*usdt_usd, pol_id[0]))
            else:
                poloniex.append((poloniex_words[i],poloniex_last_price[i], pol_id[0]))  

    except Exception:
        poloniex_answer = 0
    
    finally:
      
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO pol(br, value, idt) values %s", poloniex)
        conn.commit()
        cur.close()
        conn.close()    
        poloniex = []    
        time.sleep(10 - (time.time() - start))
