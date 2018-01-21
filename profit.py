from operator import itemgetter
import pandas as pd
import psycopg2
from psycopg2 import sql
import psycopg2.extras
import time
import ast
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
exchanges = ['wex', 'cex', 'pol', 'btfnx', 'binance', 'bittrex']
time.sleep(10)
# getting last crypto-data from databases
while True:
    start = time.time()
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
    com_usd = ast.literal_eval(cur.fetchall()[0][0])
    cur.execute("SELECT com_btc FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_btc = ast.literal_eval(cur.fetchall()[0][0])
    cur.execute("SELECT com_eth FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_eth = ast.literal_eval(cur.fetchall()[0][0])
    cur.execute("SELECT com_eur FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_eur = ast.literal_eval(cur.fetchall()[0][0])
    cur.execute("SELECT com_rub FROM usdt_com ORDER BY id DESC LIMIT 1;")
    com_rub = ast.literal_eval(cur.fetchall()[0][0])
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
           'bnn':bnn_data} # this is dict to work with

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
        itog.append((exchange_first[i],exchange_second[i],pair[i],first_price[i],second_price[i],delta[i],idt[0]))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, "INSERT INTO pairs(fex, sex, br, fvalue, svalue, delta, idt) values %s", itog)
    conn.commit()
    cur.close()
    conn.close()
    time.sleep(10 - (time.time() - start))
# preparing for output 
''' 
currencies = {'exchange1':exchange_first,'exchange2':exchange_second,'pair':pair,'first_price':first_price,'second_price':second_price,'delta':delta}
currencies = pd.DataFrame(currencies)
cols = ['exchange1','exchange2','pair','first_price','second_price','delta']
currencies = currencies[cols]
currencies.sort_values(by='delta',ascending=False)
'''
