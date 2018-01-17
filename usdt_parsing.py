import requests
import time

while True:
    start = time.time()
    try:
        r = requests.get('https://api.coinmarketcap.com/v1/ticker/',timeout=3).json()
        
        usdt_usd = r[22]['price_usd']
        usdt_answer = 1
        
    except requests.exceptions.RequestException:
        
        usdt_answer = 0
    
    finally:
    
        #write to database variable usdt_usd
        time.sleep(10 - (time.time() - start))
        