from celery.task import task
import psycopg2
import requests
import psycopg2.extras
from poloniex import Poloniex
import binance
from binance.client import Client
import crypto_parsing
import yaml
from operator import itemgetter
import pandas as pd
from psycopg2 import sql

@task
def usdt_parse():
    conn_string = "dbname='igor'"
    try:
        r = requests.get('https://api.coinmarketcap.com/v1/ticker/',timeout=3).json()
        for i in range(len(r)):
            if r[i]['symbol'] == "USDT":
                usdt_usd = r[i]['price_usd']
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

@task
def wex_parse():
    conn_string = "dbname='igor'"
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

@task
def cex_parse():
    conn_string = "dbname='igor'"
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

@task
def pol_parse():
    conn_string = "dbname='igor'"
    polo = Poloniex()
    poloniex_tickers = ['BTC_DASH','BTC_LTC','BTC_XRP','BTC_ETH','USDT_ETH','USDT_BTC','USDT_DASH','USDT_LTC','USDT_XRP','USDT_XMR',
                'XMR_LTC','XMR_DASH','BTC_ETC','ETH_ETC','USDT_ETC','BTC_ZEC','ETH_ZEC','USDT_ZEC','BTC_BCH','ETH_BCH','USDT_BCH']

    poloniex_words = ['dsh_btc','ltc_btc','xrp_btc','eth_btc','eth_usd','btc_usd','dsh_usd','ltc_usd','xrp_usd','xmr_usd',
                     'ltc_xmr','dsh_xmr','etc_btc','etc_eth','etc_usd','zec_btc','zec_eth','zec_usd','bch_btc','bch_eth','bch_usd']
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO pol_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('pol_ts_id_seq');")
        pol_id = cur.fetchone()
        cur.execute("SELECT usdt FROM usdt_com ORDER BY id DESC LIMIT 1;")
        usdt_usd = float(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()                
        poloniex_data = polo.returnTicker() 
        poloniex_last_price = []
        for i in poloniex_tickers:
            poloniex_last_price.append(poloniex_data[i]['last'])        
        poloniex_answer = 1
    except Exception:
        poloniex_answer = 0
    finally:
        poloniex = []
        for i in range(0,len(poloniex_words)):
            if poloniex_tickers[i][0:4] == 'USDT':
                poloniex.append((poloniex_words[i],poloniex_last_price[i]*usdt_usd, pol_id[0],poloniex_answer))
            else:
                poloniex.append((poloniex_words[i],poloniex_last_price[i], pol_id[0],poloniex_answer))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO pol(br, value, idt, answer) values %s", poloniex)
        conn.commit()
        cur.close()
        conn.close()    
        poloniex = []

@task
def btf_parse():
    conn_string = "dbname='igor'"
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO btfnx_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('btfnx_ts_id_seq');")
        btfnx_id = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close() 
        bitfinex_data = requests.get('''https://api.bitfinex.com/v2/tickers?symbols=tBTCUSD,tLTCUSD,tLTCBTC,tETHUSD,tETHBTC,tETCBTC,tETCUSD,tZECUSD,tZECBTC,tXMRUSD,tXMRBTC,tDSHUSD,tDSHBTC,tBTCEUR,tXRPBTC,tXRPBTC,tBCHUSD,tBCHBTC,tBCHETH''').json()
        bitfinex_answer=1
    except:
        bitfinex_answer = 0
    finally:
        bitfinex = []
        for i in bitfinex_data:
            bitfinex.append( (i[0][1:4].lower()+'_'+i[0][4:7].lower(),i[7] ))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO btfnx(br, value, idt, answer) values %s", bitfinex)
        conn.commit()
        cur.close()
        conn.close()
        
@task
def btx_parse():
    conn_string = "dbname='igor'"
    try:
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
        cur.execute("SELECT usdt FROM usdt_com ORDER BY id DESC LIMIT 1;")
        usdt_usd = float(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()
        data = requests.get('https://bittrex.com/api/v1.1/public/getmarketsummaries', timeout = 3).json()

        bittrex_s = []
        bittrex_p = []
        bittrex_values = []

        for i in data['result']:
            bittrex_s.append(i['MarketName'])
            bittrex_p.append(i['Last'])

        bittrex_price = dict(zip(bittrex_s,bittrex_p))

        for i in bittrex_pairs:
            bittrex_values.append(bittrex_price[i])

        bittrex_answer = 1

    except Exception:
        bittrex_answer = 0

    finally:
        bittrex = []
        for i in range(len(bittrex_nums)):
            if bittrex_nums[i][4:7] == 'usd':
                bittrex.append((bittrex_nums[i],float(bittrex_values[i]) * usdt_usd, bittrex_id[0],bittrex_answer,))
            else:  
                bittrex.append((bittrex_nums[i],bittrex_values[i], bittrex_id[0],bittrex_answer,))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO bittrex(br, value, idt, answer) values %s", bittrex)
        conn.commit()
        cur.close()
        conn.close()

        @task
def bnn_parse():
    conn_string = "dbname='igor'"
    client = Client('dtiWWNHXnLzD2RNe2QivRsRnrUoKWZksOgl9MwZEGuTFdyr3X2hnLZgzC5OgFp93', 
                   'LjprvlFaGf6rc42eAynezgApSbEcCYz9YGWU3EsLO9bmMxJkhDc41YrLodjnzfPR')
    binance_pairs = ['ETHBTC','LTCBTC','NEOBTC','BTCUSDT','ETHUSDT','ZECBTC','ZECETH','ETCETH','ETCBTC','DASHBTC','DASHETH','XRPBTC',
                    'XRPETH','XMRBTC','XMRETH','LTCETH','LTCUSDT']
    binance_nums = ['eth_btc','ltc_btc','neo_btc','btc_usd','eth_usd','zec_btc','zec_eth','etc_eth','etc_btc','dsh_btc','dsh_eth',
                    'xrp_btc','xrp_eth','xmr_btc','xmr_eth','ltc_eth','ltc_usd']

    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("INSERT INTO binance_ts(ts) VALUES (CURRENT_TIMESTAMP);")
        cur.execute("SELECT CURRVAL('binance_ts_id_seq');")
        binance_id = cur.fetchone()
        cur.execute("SELECT usdt FROM usdt_com ORDER BY id DESC LIMIT 1;")
        usdt_usd = float(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()
        prices = client.get_all_tickers()
        binance_values = []
        binance_p = []
        binance_s = []
        binance_right_p = []
        for i in range(0,len(prices)):
                binance_p.append(prices[i]['price'])
                binance_s.append(prices[i]['symbol'])
        binance_i = dict(zip(binance_s,binance_p))

        for i in range(0,len(binance_pairs)):
            if binance_pairs[i][3:6] == 'USD':
                binance_values.append(float(binance_i[binance_pairs[i]])*1.01)
            else:
                binance_values.append(binance_i[binance_pairs[i]])
        binance_answer = 1
    except Exception:
        binance_answer = 0
    finally:
        binance = []
        for i in range(0,len(binance_nums)):
            binance.append((binance_nums[i], binance_values[i], binance_id[0],binance_answer))
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, "INSERT INTO binance(br, value, idt, answer) values %s", binance)
        conn.commit()
        cur.close()
        conn.close()

@task
def profit_parse():
    conn_string = "dbname='igor'"
    exchanges = ['wex', 'cex', 'pol', 'btfnx', 'binance', 'bittrex']
    idt = []
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    for exchange in exchanges:
        cur.execute(sql.SQL("SELECT idt FROM {} ORDER BY id DESC LIMIT 1;").format(sql.Identifier(exchange)))
        idt.append(cur.fetchone())
    cur.execute("""
        SELECT currencies.cur, wex.value, cex.value, pol.value, btfnx.value, binance.value, bittrex.value 
        FROM currencies 
        FULL OUTER JOIN (SELECT * FROM wex WHERE wex.idt=(%s)) wex ON currencies.cur=wex.br 
        FULL OUTER JOIN (SELECT * FROM cex WHERE cex.idt=(%s)) cex ON currencies.cur=cex.br 
        FULL OUTER JOIN (SELECT * FROM pol WHERE pol.idt=(%s)) pol ON currencies.cur=pol.br 
        FULL OUTER JOIN (SELECT * FROM btfnx WHERE btfnx.idt=(%s)) btfnx ON currencies.cur=btfnx.br 
        FULL OUTER JOIN (SELECT * FROM binance WHERE binance.idt=(%s)) binance ON currencies.cur=binance.br
        FULL OUTER JOIN (SELECT * FROM bittrex WHERE bittrex.idt=(%s)) bittrex ON currencies.cur=bittrex.br;
        """, (idt[0], idt[1], idt[2], idt[3], idt[4], idt[5]))
    data = cur.fetchall()
    cur.close()
    conn.close()

    # comissions 

    out_com = {'wex':{'btc':0.001, "ltc":0.001, 'nmc':0.1, 'ppc':0.1, 'dsh':0.001, 'eth':0.003, 'bch':0.001, 'zec':0.001},
            'cex':{'btc':0, 'xrp':0, 'ppc':0, 'dsh':0, 'eth':0, 'bch':0, 'zec':0},
            'pol':{'btc':0, 'xrp':0, 'ppc':0,'dsh':0, 'eth':0, 'bch':0, 'zec':0,'ltc':0,'xmr':0,'etc':0},
            'btf':{'btc':0.0008, 'xrp':0.02,'dsh':0.01, 'eth':0.01, 'bch':0.0001, 'zec':0.001,'ltc':0.001,'xmr':0.04,'etc':0.01,'neo':0},
            'bnn':{'btc':0.001, 'xrp':0.25,'dsh':0.002, 'eth':0.01, 'zec':0.005,'ltc':0.01,'xmr':0.04,'etc':0.01,'neo':0},
            'btt':{'btc':0, 'xrp':0,'dsh':0, 'eth':0, 'zec':0,'ltc':0,'xmr':0,'etc':0,'neo':0},
            'bhb':{'krw':1000, 'btc': 0.003, 'eth':0.01, 'dsh': 0.01, 'ltc':0.01, 'etc': 0.01, 'xrp': 1, 'bch':0.005, 'xmr':0.05, 'zec':0.001}}

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("SELECT com_usd FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_usd = yaml.load(cur.fetchall()[0][0])
    cur.execute("SELECT com_btc FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_btc = yaml.load(cur.fetchall()[0][0])
    cur.execute("SELECT com_eth FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_eth = yaml.load(cur.fetchall()[0][0])
    cur.execute("SELECT com_eur FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_eur = yaml.load(cur.fetchall()[0][0])
    cur.execute("SELECT com_rub FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_rub = yaml.load(cur.fetchall()[0][0])
    cur.close()
    conn.close()

    blockchain_com = {'usd':com_usd,
                    'btc':com_btc,
                    'eth':com_eth,
                    'eur':com_eur,
                    'rub':com_usd}


    trade_com = {'wex':0.998,'cex':0.9975,'pol':0.9975,'btf':0.998,'bnn':0.999,'btt':0.9975,'bhb':0.9985}


    # transforming the data into convenient blocks
    names = []
    wex_data = []
    cex_data = []
    poloniex_data = []
    btf_data = []
    bnn_data = []
    btt_data = []
    bhb_data = []
    for i in data:
        names.append(i[0])
        wex_data.append(i[1])
        cex_data.append(i[2])
        poloniex_data.append(i[3])
        btf_data.append(i[4])
        bnn_data.append(i[5])
        btt_data.append(i[6])
       

    curr = {'names':names,'wex':wex_data,'cex':cex_data,'pol':poloniex_data,'btf':btf_data,
           'bnn':bnn_data,'btt':btt_data} # this is dict to work with

    # blocks to write final results
    exchange_first = []   
    exchange_second = []
    pair = []
    first_price = []
    second_price = []
    delta = []

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("INSERT INTO pairs_ts(ts) VALUES (CURRENT_TIMESTAMP);")
    cur.execute("SELECT CURRVAL('pairs_ts_id_seq');")
    idt = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    amount = 10 # not dollars! the amount in the currency that stands first, every time in variable letter1
    for j in curr:  # loop for choosing first exchange
        if j != 'names':
            exchange1 = j
            for o in curr:  # loop for choosing second exchange
                if o!= 'names' and o!= exchange1:
                    exchange2 = o
                    for i in range(0,len(curr['names'])): # loop for going through pairs in chosen first and second exchanges
                        letter1 = curr['names'][i][0:3]
                        letter2 = curr['names'][i][4:7]
                        if curr[exchange1][i]!=None and curr[exchange2][i]!=None:
                            exchange_first.append(exchange1)
                            exchange_second.append(exchange2)
                            pair.append(curr['names'][i])
                            first_price.append(curr[exchange1][i])
                            second_price.append(curr[exchange2][i])
                            delta.append( ((amount * trade_com[exchange1] - out_com[exchange1][letter1]*amount)*float(curr[exchange1][i]) - float(blockchain_com[letter2][letter1]))/float(curr[exchange1][i]) * trade_com[exchange2] * float(curr[exchange2][i])/float(curr[exchange1][i])/amount)
    itog=[]
    for i in range(len(exchange_first)):
        itog.append((exchange_first[i],exchange_second[i],pair[i],first_price[i],second_price[i],round(delta[i],2),idt[0]))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, "INSERT INTO pairs(fex, sex, br, fvalue, svalue, delta, idt) values %s", itog)
    conn.commit()
    cur.close()
    conn.close()
