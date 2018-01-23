from celery import Celery
import psycopg2
import requests
import psycopg2.extras
import time

app = Celery('proj', broker='amqp://')

@app.task
def cex_parse():
    conn_string = "dbname='igor' user='server' password='Chordify2811' host='138.197.179.83'"
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO cex_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('cex_ts_id_seq');")
        cex_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        cex_data1 = requests.get('https://cex.io/api/tickers/BTC/USD',timeout=1)
        cex_data2 = requests.get('https://cex.io/api/tickers/EUR/GBP',timeout=1)
        cex_data3 = requests.get('https://cex.io/api/tickers/BTC/RUB',timeout=1)
        cex_answer = 1
        cex_nums = []
        cex_values = []
        for i in range(0,len(cex_data1.json()['data'])):
            cex_nums.append(cex_data1.json()['data'][i]['pair'].lower().replace(':', '_'))
            cex_values.append(cex_data1.json()['data'][i]['last'])
        for i in range(0,len(cex_data2.json()['data'])):
            if cex_data2.json()['data'][i]['pair'].lower().replace(':', '_') not in cex_nums:
                cex_nums.append(cex_data2.json()['data'][i]['pair'].lower().replace(':', '_'))
                cex_values.append(cex_data2.json()['data'][i]['last'])
        for i in range(0,len(cex_data3.json()['data'])):
            if cex_data3.json()['data'][i]['pair'].lower().replace(':', '_') not in cex_nums:
                cex_nums.append(cex_data3.json()['data'][i]['pair'].lower().replace(':', '_'))
                cex_values.append(cex_data3.json()['data'][i]['last'])
        for i in range(0,len(cex_nums)):
            if cex_nums[i][0:4] == 'dash':
                b = cex_nums[i][4:8]
                cex_nums[i] = 'dsh'+b
    except Exception:
        cex_answer = 0
    finally:
        cex = []
        for i in range(0,len(cex_nums)):
            cex.append((cex_nums[i],cex_values[i], cex_id[0],cex_answer))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO cex(br, value, idt, answer) values %s", cex)
        conn.commit()
        cur.close()
        conn.close()

        
        
        
@app.task
def wex_parse():
    conn_string = "dbname='igor' user='server' password='Chordify2811' host='138.197.179.83'"
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO wex_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('wex_ts_id_seq');")
        wex_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        wex_data = requests.get(
                'https://wex.nz/api/3/ticker/btc_usd-btc_eur-dsh_btc-dsh_usd-dsh_eur-eth_usd-eth_btc-eth_eur-bch_usd-bch_eur-bch_btc-zec_btc-zec_usd-btc_rur-ltc_btc-ltc_usd-ltc_rur-ltc_eur-nmc_btc-nmc_usd-nvc_btc-nvc_usd-usd_rur-eur_usd-eur_rur-ppc_btc-ppc_usd-dsh_rur-dsh_ltc-eth_ltc-eth_rur-bch_rur-bch_ltc-bch_dsh?ignore_invalid=1',timeout=2)
        wex_answer = 1
        words = ['bch_btc','bch_eur','bch_usd','btc_eur','btc_usd','dsh_btc','dsh_eur','dsh_usd','eth_btc','eth_eur','eth_usd',
         'zec_btc', 'zec_usd','btc_rur','ltc_btc','ltc_usd','ltc_rur','ltc_eur','nmc_btc','nmc_usd','nvc_btc','nvc_usd',
        'usd_rur','eur_usd','eur_rur','ppc_btc','ppc_usd','dsh_rur','dsh_ltc','eth_ltc','eth_rur','bch_rur','bch_ltc','bch_dsh']
        d = []
        for x in words:
            d.append(wex_data.json()[x]['last'])
        for i in range(0,len(words)):
            if words[i][4:7] == 'rur':
                words[i] = words[i][0:4] + 'rub' 
    except requests.exceptions.Timeout:
        wex_answer = 0
    finally:
        wex = []
        for i in range(0,len(words)):
            wex.append((words[i], d[i], wex_id[0],wex_answer))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO wex(br, value, idt, answer) values %s", wex)
        conn.commit()
        cur.close()
        conn.close()
