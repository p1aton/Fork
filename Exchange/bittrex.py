import requests
import time
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
usdt_usd = 1.01

while True:
    try:
        start = time.time()
        bittrex_s = []
        bittrex_p = []
        bittrex_values = []
        bittrex = []
        
        # 2 следующие переменные можно же вынести в глобальные? вроде бы в прошлый раз Игорь говорил, что нет, но я забыл почему
        bittrex_pairs = ['BTC-DASH','BTC-ETC','BTC-ETH','BTC-LTC','BTC-NEO','BTC-XMR','BTC-XRP','BTC-ZEC','ETH-DASH','ETH-LTC',
                          'ETH-NEO','ETH-XMR','ETH-XRP','ETH-ZEC','USDT-BTC','USDT-DASH','USDT-ETC','USDT-ETH','USDT-LTC','USDT-NEO',
                         'USDT-XMR','USDT-XRP','USDT-ZEC']
        bittrex_nums = ['dsh_btc','etc_btc','eth_btc','ltc_btc','neo_btc','xmr_btc','xrp_btc','zec_btc','dsh_eth','ltc_eth',
                        'neo_eth','xmr_eth','xrp_eth','zec_eth','btc_usd','dsh_usd','etc_usd','eth_usd','ltc_usd','neo_usd',
                        'xmr_usd','xrp_usd','zec_usd']
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO bittrex_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('bittrex_ts_id_seq');")
        bittrex_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        data = requests.get('https://bittrex.com/api/v1.1/public/getmarketsummaries').json()

        for i in data['result']:
            bittrex_s.append(i['MarketName'])
            bittrex_p.append(i['Last'])

        bittrex_price = dict(zip(bittrex_s,bittrex_p))

        for i in bittrex_pairs:
            bittrex_values.append(bittrex_price[i])
        for i in range(len(bittrex_nums)):
            if bittrex_nums[i][4:7] == 'usd':
                bittrex.append((bittrex_nums[i],float(bittrex_values[i]) * usdt_usd, bittrex_id[0],))
            else:  
                bittrex.append((bittrex_nums[i],bittrex_values[i], bittrex_id[0],))
        bittrex_answer = 1
        print(bittrex)
    except Exception:
        bittrex_answer = 0
    
    finally:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO bittrex(br, value, idt) values %s", bittrex)
        conn.commit()
        cur.close()
        conn.close()
        
        time.sleep(10 - (time.time() - start ))
