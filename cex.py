import requests
import psycopg2
import psycopg2.extras
import time
conn_string = "dbname='igor'"
while True:
    start = time.time()
    cex_data1 = requests.get('https://cex.io/api/tickers/BTC/USD')
    cex_data2 = requests.get('https://cex.io/api/tickers/EUR/GBP')
    cex_data3 = requests.get('https://cex.io/api/tickers/BTC/RUB')
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("INSERT INTO cex_ts(ts) VALUES (CURRENT_TIMESTAMP);")
    cur.execute("SELECT CURRVAL('cex_ts_id_seq');")
    cex_id = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
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
                b = cex_nums[i][4:8]
                cex_nums[i] = 'dsh'+b
        cex = []
        for i in range(0,len(cex_nums)):
            cex.append((cex_nums[i],cex_values[i], cex_id[0]))
        records_list_template = ','.join(['%s'] * len(cex))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO cex(br, value, idt) values %s", cex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10-(time.time() - start))
    else:
        cex_answer = 'no connection'
        cex = []
        for i in range(0,len(cex_nums)):
            cex.append((cex_nums[i],cex_values[i], cex_id[0]))
        records_list_template = ','.join(['%s'] * len(cex))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO cex(br, value, idt) values %s", cex)
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(10-(time.time() - start))
