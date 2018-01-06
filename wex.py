import requests
import psycopg2
import psycopg2.extras
import time
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
while True:
    start = time.time()
    wex_data = requests.get(
                'https://wex.nz/api/3/ticker/btc_usd-btc_eur-dsh_btc-dsh_usd-dsh_eur-eth_usd-eth_btc-eth_eur-bch_usd-bch_eur-bch_btc-zec_btc-zec_usd-btc_rur-ltc_btc-ltc_usd-ltc_rur-ltc_eur-nmc_btc-nmc_usd-nvc_btc-nvc_usd-usd_rur-eur_usd-eur_rur-ppc_btc-ppc_usd-dsh_rur-dsh_ltc-eth_ltc-eth_rur-bch_rur-bch_ltc-bch_dsh?ignore_invalid=1')
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("INSERT INTO wex_ts(ts) VALUES (CURRENT_TIMESTAMP);")
    cur.execute("SELECT CURRVAL('wex_ts_id_seq');")
    wex_id = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if wex_data.status_code == 200:
        wex_answer = 'ok'
        words = ['bch_btc','bch_eur','bch_usd','btc_eur','btc_usd','dsh_btc','dsh_eur','dsh_usd','eth_btc','eth_eur','eth_usd',
             'zec_btc', 'zec_usd','btc_rur','ltc_btc','ltc_usd','ltc_rur','ltc_eur','nmc_btc','nmc_usd','nvc_btc','nvc_usd',
            'usd_rur','eur_usd','eur_rur','ppc_btc','ppc_usd','dsh_rur','dsh_ltc','eth_ltc','eth_rur','bch_rur','bch_ltc','bch_dsh']
        d = []
        wex = []
        for x in words:
            d.append(wex_data.json()[x]['last'])
        for i in range(0,len(words)):
            if words[i][4:7] == 'rur':
                words[i] = words[i][0:4] + 'rub'
        for i in range(0,len(words)):
            wex.append((words[i], d[i], wex_id[0],))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO wex(br, value, idt) values %s", wex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10 - (time.time() - start))
    else:
        wex_answer = 'no connection'
        wex = []
        for i in range(0,len(words)):
            wex.append((words[i], d[i], wex_id[0],))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO wex(br, value, idt) values %s", wex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10 - (time.time() - start))
