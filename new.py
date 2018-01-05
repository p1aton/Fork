import time
import requests
import psycopg2
while True:
    start = time.time()
    wex_data = requests.get(
              'https://wex.nz/api/3/ticker/btc_usd-btc_eur-dsh_btc-dsh_usd-dsh_eur-eth_usd-eth_btc-eth_eur-bch_usd-bch_eur-bch_btc-zec_btc-zec_usd-btc_rur-ltc_btc-ltc_usd-ltc_rur-ltc_eur-nmc_btc-nmc_usd-nvc_btc-nvc_usd-usd_rur-eur_usd-eur_rur-ppc_btc-ppc_usd-dsh_rur-dsh_ltc-eth_ltc-eth_rur-bch_rur-bch_ltc-bch_dsh?ignore_invalid=1')
    if wex_data.status_code == 200:
        wex_answer = 'ok'
        words = ['bch_btc','bch_eur','bch_usd','btc_eur','btc_usd','dsh_btc','dsh_eur','dsh_usd','eth_btc','eth_eur','eth_usd',
           'zec_btc', 'zec_usd','btc_rur','ltc_btc','ltc_usd','ltc_rur','ltc_eur','nmc_btc','nmc_usd','nvc_btc','nvc_usd',
          'usd_rur','eur_usd','eur_rur','ppc_btc','ppc_usd','dsh_rur','dsh_ltc','eth_ltc','eth_rur','bch_rur','bch_ltc','bch_dsh']
        d = []
        wex = []
        conn = psycopg2.connect(dbname="igor")
        cur = conn.cursor()
        cur.execute("INSERT INTO ts VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('ts_idt_seq');")
        idt = cur.fetchone()
        for x in words:
            d.append(wex_data.json()[x]['last'])
        for i in range(0,len(words)):
            if words[i][4:7] == 'rur':
                words[i] = words[i][0:4] + 'rub'
        
        for i in range(0,len(words)):
            wex.append((words[i], d[i], idt[0]))
        records_list_template = ','.join(['%s'] * len(wex))
        insert_query = 'INSERT INTO wex(br, value, idt) values {}'.format(records_list_template)
        cur.execute(insert_query, wex)
        conn.commit()
        cur.close()
        conn.close()
        else:
            wex_answer = 'no connection'
            time.sleep(10 - (time.time() - start))
        


    cex_data1 = requests.get('https://cex.io/api/tickers/BTC/USD')
    cex_data2 = requests.get('https://cex.io/api/tickers/EUR/GBP')
    cex_data3 = requests.get('https://cex.io/api/tickers/BTC/RUB')

    if (cex_data1.status_code == 200) and (cex_data2.status_code == 200) and (cex_data3.status_code == 200):
        cex_answer = 'ok'
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
                cex_nums[i] = 'dsh'+cex_nums[0][3:7]
        cex = []
        conn = psycopg2.connect(dbname="igor")
        cur = conn.cursor()
        for i in range(0,len(cex_nums)):
            cex.append((cex_nums[i],cex_values[i], idt[0]))
        records_list_template = ','.join(['%s'] * len(cex))
        insert_query = 'INSERT INTO cex(br, value, idt) values {}'.format(records_list_template)
        cur.execute(insert_query, cex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10 - (time.time() - start))
    else:
        cex_answer = 'no connection'
        time.sleep(10 - (time.time() - start))
