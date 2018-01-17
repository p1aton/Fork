from operator import itemgetter
import pandas as pd
import crypto_parsing
import psycopg2
import psycopg2.extras
import config
conn_string = config.conn_string

# getting last crypto-data from databases
while True:
    start = time.time()
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("SELECT id FROM wex_ts ORDER BY id DESC LIMIT 1;")
    idw = cur.fetchone()
    cur.execute("SELECT id FROM cex_ts ORDER BY id DESC LIMIT 1;")
    idc = cur.fetchone()
    cur.execute("SELECT id FROM pol_ts ORDER BY id DESC LIMIT 1;")
    idp = cur.fetchone()
    cur.execute("SELECT id FROM btfnx_ts ORDER BY id DESC LIMIT 1;")
    idbtfnx = cur.fetchone()
    cur.execute("SELECT id FROM binance_ts ORDER BY id DESC LIMIT 1;")
    idbinance = cur.fetchone()
    cur.execute("SELECT id FROM bittrex_ts ORDER BY id DESC LIMIT 1;")
    idbittrex = cur.fetchone()
    cur.execute("""
        SELECT currencies.cur, wex.value, cex.value, pol.value, btfnx.value, binance.value, bittrex.value 
        FROM currencies 
        FULL OUTER JOIN (SELECT * FROM wex WHERE wex.idt=(%s)) wex ON currencies.cur=wex.br 
        FULL OUTER JOIN (SELECT * FROM cex WHERE cex.idt=(%s)) cex ON currencies.cur=cex.br 
        FULL OUTER JOIN (SELECT * FROM pol WHERE pol.idt=(%s)) pol ON currencies.cur=pol.br 
        FULL OUTER JOIN (SELECT * FROM btfnx WHERE btfnx.idt=(%s)) btfnx ON currencies.cur=btfnx.br 
        FULL OUTER JOIN (SELECT * FROM binance WHERE binance.idt=(%s)) binance ON currencies.cur=binance.br
        FULL OUTER JOIN (SELECT * FROM bittrex WHERE bittrex.idt=(%s)) bittrex ON currencies.cur=bittrex.br;
        """, (idw, idc, idp, idbtfnx, idbinance, idbittrex))
    data = cur.fetchall()
    cur.close()
    conn.close()

    # comissions 

    out_com = {'wex':{'btc':0.001, "ltc":0.001, 'nmc':0.1, 'ppc':0.1, 'dsh':0.001, 'eth':0.003, 'bch':0.001, 'zec':0.001},
            'cex':{'btc':0, 'xrp':0, 'ppc':0, 'dsh':0, 'eth':0, 'bch':0, 'zec':0},
            'pol':{'btc':0, 'xrp':0, 'ppc':0,'dsh':0, 'eth':0, 'bch':0, 'zec':0,'ltc':0,'xmr':0,'etc':0},
            'btf':{'btc':0.0008, 'xrp':0.02,'dsh':0.01, 'eth':0.01, 'bch':0.0001, 'zec':0.001,'ltc':0.001,'xmr':0.04,'etc':0.01,'neo':0},
            'bnn':{'btc':0.001, 'xrp':0.25,'dsh':0.002, 'eth':0.01, 'zec':0.005,'ltc':0.01,'xmr':0.04,'etc':0.01,'neo':0},
            'btt':{'btc':0, 'xrp':0,'dsh':0, 'eth':0, 'zec':0,'ltc':0,'xmr':0,'etc':0,'neo':0}}

    com_usd,com_btc,com_eth,com_eur,com_rub = crypto_parsing.get_all_comissions()

    blockchain_com = {'usd':com_usd,
                    'btc':com_btc,
                    'eth':com_eth,
                    'eur':com_eur,
                    'rub':com_usd}


    trade_com = {'wex':0.998,'cex':0.9975,'pol':0.9975,'btf':0.998,'bnn':0.999,'btt':0.9975}


    # transforming the data into convenient blocks
    names = []
    wex_data = []
    cex_data = []
    poloniex_data = []
    btf_data = []
    bnn_data = []
    btt_data = []
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
    wex_id = cur.fetchone()
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
                            print(exchange1,exchange2,letter1,letter2)
                            exchange_first.append(exchange1)
                            exchange_second.append(exchange2)
                            pair.append(curr['names'][i])
                            first_price.append(curr[exchange1][i])
                            second_price.append(curr[exchange2][i])
                            delta.append( ((amount * trade_com[exchange1] - out_com[exchange1][letter1]*amount)*float(curr[exchange1][i]) - float(blockchain_com[letter2][letter1]))/float(curr[exchange1][i]) * trade_com[exchange2] * float(curr[exchange2][i])/float(curr[exchange1][i])/amount)
    itog=[]
    for i in range(len(exchange1)):
        itog.append((exchange_first[i],exchange_second[i],pair[i],first_price[i],second_price[i],delta[i],idt[0]))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, "INSERT INTO pairs(fex, sex, br, fvalue, svalue, delta, idt) values %s", itog)
    conn.commit()
    cur.close()
    conn.close()
    print(time.time() - start)
    time.sleep(10 - (time.time() - start))
# preparing for output 
''' 
currencies = {'exchange1':exchange_first,'exchange2':exchange_second,'pair':pair,'first_price':first_price,'second_price':second_price,'delta':delta}
currencies = pd.DataFrame(currencies)
cols = ['exchange1','exchange2','pair','first_price','second_price','delta']
currencies = currencies[cols]
currencies.sort_values(by='delta',ascending=False)
'''
