from operator import itemgetter
import pandas as pd
import psycopg2
import psycopg2.extras

# getting last crypto-data from databases
conn_string = "dbname='igor' user='server' password='Chordify2811' host='138.197.179.83'"
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
cur.execute("SELECT currencies.cur, wex.value, cex.value, pol.value, btfnx.value, binance.value FROM currencies FULL OUTER JOIN (SELECT * FROM wex WHERE wex.idt=(%s)) wex ON currencies.cur=wex.br FULL OUTER JOIN (SELECT * FROM cex WHERE cex.idt=(%s)) cex ON currencies.cur=cex.br FULL OUTER JOIN (SELECT * FROM pol WHERE pol.idt=(%s)) pol ON currencies.cur=pol.br FULL OUTER JOIN (SELECT * FROM btfnx WHERE btfnx.idt=(%s)) btfnx ON currencies.cur=btfnx.br FULL OUTER JOIN (SELECT * FROM binance WHERE binance.idt=(%s)) binance ON currencies.cur=binance.br;", (idw, idc, idp, idbtfnx, idbinance))
data = cur.fetchall()
cur.close()
conn.close()

# comissions 

out_com = {'wex':{'btc':0.001, "ltc":0.001, 'nmc':0.1, 'ppc':0.1, 'dsh':0.001, 'eth':0.003, 'bch':0.001, 'zec':0.001},
          'cex':{'btc':0, 'xrp':0, 'ppc':0, 'dsh':0, 'eth':0, 'bch':0, 'zec':0},
          'pol':{'btc':0, 'xrp':0, 'ppc':0,'dsh':0, 'eth':0, 'bch':0, 'zec':0,'ltc':0,'xmr':0,'etc':0}}
blockhain_com = {'usd':{'btc':28.75,'bch':0.418,'eth':2.596,'ltc':0.295,'xrp':0.0161,'dsh':0.441,'xmr':10.476,'zec':0.0006,'ppc':0.0923,
               'nmc':0.0077,'etc':0.0275},
                'btc':{'btc':0.001982,'bch':0.000028,'eth':0.000179,'ltc':0.00002,'xrp':0.000001,'dsh':0.00003,'xmr':0.00072,
                       'zec':0.00000004,'ppc':0.0000063655,'nmc':0.0000005310,'etc':0.0000018966},
                'eur':{'btc':23.575,'bch':0.34276,'eth':2.12872,'ltc':0.2419,'xrp':0.0132,'dsh':0.36162,'xmr':8.59032,
                       'zec':0.000492,'ppc':0.075686,'nmc':0.006314,'etc':0.02255},
                'rub':{'btc':1628.6875,'bch':23.6797,'eth':147.0634,'ltc':16.71175,'xrp':0.912,'dsh':24.98265,'xmr':593.4654,
                       'zec':0.03399,'ppc':5.228795,'nmc':0.4362,'etc':1.557875}}
trade_com = {'wex':0.998,'cex':0.9975,'pol':0.9975}

com = {'wex':0.998,'cex':0.9975,'pol':0.9975}


# transforming the data into convenient blocks
wex_data = []
cex_data = []
poloniex_data = []
names = []
for i in data:
    names.append(i[0])
    wex_data.append(i[1])
    cex_data.append(i[2])
    poloniex_data.append(i[3])

curr = {'names':names,'wex':wex_data,'cex':cex_data,'pol':poloniex_data} # this is dict to work with


# blocks to write final results
exchange_first = []   
exchange_second = []
pair = []
first_price = []
second_price = []
delta = []

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
                        delta.append( ((amount * trade_com[exchange1] - out_com[exchange1][letter1]*amount)*float(curr[exchange1][i]) - blockhain_com[letter2][letter1])/float(curr[exchange1][i]) * trade_com[exchange2] * float(curr[exchange2][i])/float(curr[exchange1][i])/amount)

# preparing for output                  
currencies = {'exchange1':exchange_first,'exchange2':exchange_second,'pair':pair,'first_price':first_price,'second_price':second_price,'delta':delta}
currencies = pd.DataFrame(currencies)
cols = ['exchange1','exchange2','pair','first_price','second_price','delta']
currencies = currencies[cols]
currencies.sort_values(by='delta',ascending=False)
