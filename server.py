import time
import requests
import psycopg2

idt = 1
while True:

    wex_data = requests.get(
                'https://wex.nz/api/3/ticker/btc_usd-btc_eur-dsh_btc-dsh_usd-dsh_eur-eth_usd-eth_btc-eth_eur-bch_usd-bch_eur-bch_btc-zec_btc-zec_usd')
    cex_data = requests.get('https://cex.io/api/tickers/BTC/USD')
    cex_data1 = requests.get('https://cex.io/api/tickers/BTC/EUR')

    wex_bch_btc = wex_data.json()['bch_btc']['last']
    wex_bch_eur = wex_data.json()['bch_eur']['last']
    wex_bch_usd = wex_data.json()['bch_usd']['last']
    wex_btc_eur = wex_data.json()['btc_eur']['last']
    wex_btc_usd = wex_data.json()['btc_usd']['last']
    wex_dsh_btc = wex_data.json()['dsh_btc']['last']
    wex_dsh_eur = wex_data.json()['dsh_eur']['last']
    wex_dsh_usd = wex_data.json()['dsh_usd']['last']
    wex_eth_btc = wex_data.json()['eth_btc']['last']
    wex_eth_eur = wex_data.json()['eth_eur']['last']
    wex_eth_usd = wex_data.json()['eth_usd']['last']
    wex_zec_btc = wex_data.json()['zec_btc']['last']
    wex_zec_usd = wex_data.json()['zec_usd']['last']

    cex_bch_btc = float(cex_data.json()['data'][8]['last'])
    cex_bch_eur = float(cex_data1.json()['data'][2]['last'])
    cex_bch_usd = float(cex_data.json()['data'][2]['last'])
    cex_btc_eur = float(cex_data1.json()['data'][0]['last'])
    cex_btc_usd = float(cex_data.json()['data'][0]['last'])
    cex_dsh_btc = float(cex_data.json()['data'][10]['last'])
    cex_dsh_eur = float(cex_data1.json()['data'][4]['last'])
    cex_dsh_usd = float(cex_data.json()['data'][4]['last'])
    cex_eth_btc = float(cex_data.json()['data'][7]['last'])
    cex_eth_eur = float(cex_data1.json()['data'][1]['last'])
    cex_eth_usd = float(cex_data.json()['data'][1]['last'])
    cex_zec_btc = float(cex_data.json()['data'][12]['last'])
    cex_zec_usd = float(cex_data.json()['data'][6]['last'])

    bch_btc = round((cex_bch_btc / wex_bch_btc - 1) * 100, 2)
    bch_eur = round((cex_bch_eur / wex_bch_eur - 1) * 100, 2)
    bch_usd = round((cex_bch_usd / wex_bch_usd - 1) * 100, 2)
    btc_eur = round((cex_btc_eur / wex_btc_eur - 1) * 100, 2)
    btc_usd = round((cex_btc_usd / wex_btc_usd - 1) * 100, 2)
    dsh_btc = round((cex_dsh_btc / wex_dsh_btc - 1) * 100, 2)
    dsh_eur = round((cex_dsh_eur / wex_dsh_eur - 1) * 100, 2)
    dsh_usd = round((cex_dsh_usd / wex_dsh_usd - 1) * 100, 2)
    eth_btc = round((cex_eth_btc / wex_eth_btc - 1) * 100, 2)
    eth_eur = round((cex_eth_eur / wex_eth_eur - 1) * 100, 2)
    eth_usd = round((cex_eth_usd / wex_eth_usd - 1) * 100, 2)
    zec_btc = round((cex_zec_btc / wex_zec_btc - 1) * 100, 2)
    zec_usd = round((cex_zec_usd / wex_zec_usd - 1) * 100, 2)
    
    conn = psycopg2.connect(dbname="igor", user="igor", password="Chordify2811", host="138.197.179.83")

    cur = conn.cursor()
    cur.execute("SET TIME ZONE 'Europe/Moscow';")
    cur.execute("INSERT INTO ts VALUES (CURRENT_TIMESTAMP, %s);" % idt)
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('bch_btc', %s, %s);", (bch_btc, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('bch_eur', %s, %s);", (bch_eur, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('bch_usd', %s, %s);", (bch_usd, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('btc_eur', %s, %s);", (btc_eur, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('btc_usd', %s, %s);", (btc_usd, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('dsh_btc', %s, %s);", (dsh_btc, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('dsh_eur', %s, %s);", (dsh_eur, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('dsh_usd', %s, %s);", (dsh_usd, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('eth_btc', %s, %s);", (eth_btc, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('eth_eur', %s, %s);", (eth_eur, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('eth_usd', %s, %s);", (eth_usd, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('zec_btc', %s, %s);", (zec_btc, idt))
    cur.execute("INSERT INTO birga(br, delta, idt) VALUES('zec_usd', %s, %s);", (zec_usd, idt))
    idt = idt + 1
    conn.commit()

    cur.close()
    conn.close()

    time.sleep(10 - time.time() % 1)
