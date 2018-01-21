import requests
import time
import psycopg2
import crypto_parsing
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"

while True:
    start = time.time()
    try:
        r = requests.get('https://api.coinmarketcap.com/v1/ticker/',timeout=3).json()
        for i in range(len(r)):
            if r[i]['symbol'] == "USDT":
                usdtd_usd = r[i]['price_usd']
        com_usd,com_btc,com_eth,com_eur,com_rub = crypto_parsing.get_all_comissions()
        usdt_answer = 1
        com_answer = 1
        
    except Exception:
        usdt_answer = 0
        com_answer = 0
    finally:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("""INSERT INTO usdt_com(usdt, com_usd, com_btc, com_eth, com_eur, com_rub, ts)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);""", (float(usdt_usd), str(com_usd), str(com_btc), str(com_eth), str(com_eur), str(com_rub)))
        conn.commit()
        cur.close()
        conn.close()
    time.sleep(60 - (time.time() - start))
