import time
import requests
import psycopg2

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

    conn = psycopg2.connect(dbname="igor")

    cur = conn.cursor()
    cur.execute("INSERT INTO ts VALUES (CURRENT_TIMESTAMP);")
    cur.execute("SELECT CURRVAL('ts_idt_seq');")
    idt = cur.fetchone()
    data = [('bch_btc', bch_btc, idt[0]), ('bch_eur', bch_eur, idt[0]), ('bch_usd', bch_usd, idt[0]), ('btc_eur', btc_eur, idt[0]),
        ('btc_usd', btc_usd, idt[0]), ('dsh_btc', dsh_btc, idt[0]), ('dsh_eur', dsh_eur, idt[0]), ('dsh_usd', dsh_usd, idt[0]),
        ('eth_btc', eth_btc, idt[0]), ('eth_eur', eth_eur, idt[0]), ('eth_usd', eth_usd, idt[0]), ('zec_btc', zec_btc, idt[0]),
        ('zec_usd', zec_usd, idt[0])]
    records_list_template = ','.join(['%s'] * len(data))
    insert_query = 'INSERT INTO birga(br, delta, idt) values {}'.format(records_list_template)
    cur.execute(insert_query, data)
    conn.commit()

    cur.close()
    conn.close()

    time.sleep(10 - time.time() % 1)
