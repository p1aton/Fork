import requests
import time
import psycopg2
import crypto_parsing
conn_string = "dbname='igor'"

while True:
    start = time.time()
    try:
        r = requests.get('https://api.coinmarketcap.com/v1/ticker/',timeout=3).json()
        com_usd,com_btc,com_eth,com_eur,com_rub = crypto_parsing.get_all_comissions()
        
        
        usdt_usd = r[22]['price_usd']
        usdt_answer = 1
        com_answer = 1
        
    except Exception:
        
        usdt_answer = 0
        com_answer = 0
    
    
    finally:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO usdt(value, ts) VALUES (%s, CURRENT_TIMESTAMP);", [usdt_usd])
        conn.commit()
        cur.close()
        conn.close()
    time.sleep(60 - (time.time() - start))
        
