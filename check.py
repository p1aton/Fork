import pandas
import datetime
import krakenex
import pandas as pd
import time
#https://python3-krakenex.readthedocs.io/en/latest/
#https://www.kraken.com/help/api#get-asset-info

k = krakenex.API(key='en0i96UpL6N62X23/hxVgAAfNIZMWhp78LFMc3441Yor4NeCjSIV/OhM',secret='LnLXo4FQqsrhNqubPU9F7VBifC75dNa4yC+QjMjXhQbYoc3Sr7BgFfBYLVsFyWgn7kLj6Xk9c/vSiVtT/hx/YA==')
x = 0
for _ in range(10):
    data_OHLC = k.query_public('OHLC',{'pair': 'XBTUSD','interval':5})
    ohlc_time = []
    ohlc_open=[]
    ohlc_close=[]
    ohlc_high=[]
    ohlc_low=[]
    ohlc_volume=[]
    ohlc_price = []
    for i in range(0,len(data_OHLC['result']['XXBTZUSD'])):
        ohlc_time.append(datetime.datetime.fromtimestamp(data_OHLC['result']['XXBTZUSD'][i][0]).strftime('%Y-%m-%d %H:%M:%S'))
        ohlc_open.append(data_OHLC['result']['XXBTZUSD'][i][1])
        ohlc_high.append(data_OHLC['result']['XXBTZUSD'][i][2])
        ohlc_low.append(data_OHLC['result']['XXBTZUSD'][i][3])
        ohlc_close.append(data_OHLC['result']['XXBTZUSD'][i][4])
        ohlc_price.append(data_OHLC['result']['XXBTZUSD'][i][5])
        ohlc_volume.append(data_OHLC['result']['XXBTZUSD'][i][6])

    ohlc_dict = {'ohlc_time':ohlc_time,'ohlc_price':ohlc_price,'ohlc_open':ohlc_open,
                 'ohlc_close':ohlc_close,'ohlc_low':ohlc_low,'ohlc_high':ohlc_high,'ohlc_volume':ohlc_volume}
    ohlc_df = pd.DataFrame(ohlc_dict)
    
    x += 1
    ohlc_df.to_csv('data{}.csv'.format(x))
    
    
    time.sleep(6 - time.time() % 1)