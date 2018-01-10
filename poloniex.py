from poloniex import Poloniex
import psycopg2
import psycopg2.extras
import time

polo = Poloniex()
usdt_usd = 1.01 # парсить актуальную с poloniex

# формат, который выдает poloniex
poloniex_tickers = ['BTC_DASH','BTC_LTC','BTC_XRP','BTC_ETH','USDT_ETH','USDT_BTC','USDT_DASH','USDT_LTC','USDT_XRP','USDT_XMR',
            'XMR_LTC','XMR_DASH','BTC_ETC','ETH_ETC','USDT_ETC','BTC_ZEC','ETH_ZEC','USDT_ZEC','BTC_BCH','ETH_BCH','USDT_BCH']

#формат, который нужен нам
poloniex_words = ['dash_btc','ltc_btc','xrp_btc','eth_btc','eth_usd','btc_usd','dsh_usd','ltc_usd','xrp_usd','xmr_usd',
                 'ltc_xmr','dsh_xmr','etc_btc','etc_eth','etc_usd','zec_btc','zec_eth','zec_usd','bch_btc','bch_eth','bch_usd']
while True:
    start = time.time()
    try:
        poloniex_data = polo.returnTicker()
        if poloniex_data != 0: #важное условие, чтобы в случае except предыдущие значения не стерлись
            poloniex_last_price = []
            poloniex = []
        
        # poloniex_id = cur.fetchone()
        
        for i in polo_tickers:
            poloniex_last_price.append(poloniex_data[i]['last'])
            
        for i in range(0,len(poloniex_words)):
            if polo_tickers[i][0:4] == 'USDT':
                poloniex.append((poloniex_words[i],poloniex_last_price[i]*usdt_usd,))  # дописать сюда внутри poloniex_id[0]
            else:
                poloniex.append((poloniex_words[i],poloniex_last_price[i],)) # сюда тоже дописать poloniex_id[0]
        
        poloniex_answer = 'ok'
        time.sleep(10 - (time.time() - start))
        
    except:
        poloniex_answer = 'no connection'
        poloniex = []
        for i in range(0,len(poloniex_words)):
            if polo_tickers[i][0:4] == 'USDT':
                poloniex.append((poloniex_words[i],poloniex_last_price[i]*usdt_usd,))  # дописать сюда внутри poloniex_id[0]
            else:
                poloniex.append((poloniex_words[i],poloniex_last_price[i],)) # сюда тоже дописать poloniex_id[0]
        
        
            
        #код для баз данных    
        
        
        time.sleep(10 - (time.time() - start))