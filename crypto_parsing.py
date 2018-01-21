from forex_python.converter import CurrencyRates 
import re
import pandas as pd
import requests
from bs4 import  BeautifulSoup

# reflesh list of cryptos
'''


#get list of cryptocurs


crypts = []
wsite = requests.get('https://bitinfocharts.com/comparison/btc-median_transaction_fee.html')
soup = BeautifulSoup(wsite.text, 'lxml')
s1 = soup.find('div', {'style':'margin-left:5px; clear:both; text-align:left;overflow: hidden; height:20px;'})
for s in s1:
    try:
        crypts.append(s['data-coin'])
    except:
        pass
'''

crypts = ['btc', 'eth', 'xrp', 'bch', 'ltc', 'dsh', 'xmr', 'btg', 'etc', 'zec', 'doge', 'rdd', 'vtc', 'ppc', 'ftc', 
          'nmc', 'blk', 'aur', 'nvc', 'qrk', 'mec']

c = CurrencyRates()

def full_cryptocurrs_data(type_): 
    try:
        r1 = requests.get('https://bitinfocharts.com/comparison/{type_}-{cryptos}.html'.format(type_ = type_, cryptos = str(crypts[0:10]).replace("', '", '-').replace("'", '').replace('[', '').replace(']', '')))
        r2 = BeautifulSoup(r1.text, 'lxml')
        r3 = re.findall(r"\[new Date(.*?)\]",str(r2))
        r4 = r3[-1].replace('"', '').replace('/', '-').replace('(', '').replace(')', '').replace('null', 'NaN').split(',')[1:]
        r11 = requests.get('https://bitinfocharts.com/comparison/{type_}-{cryptos}.html'.format(type_ = type_, cryptos = str(crypts[10:21]).replace("', '", '-').replace("'", '').replace('[', '').replace(']', '')))
        r22 = BeautifulSoup(r11.text, 'lxml')
        r33 = re.findall(r"\[new Date(.*?)\]",str(r22))
        r44 = r33[-1].replace('"', '').replace('/', '-').replace('(', '').replace(')', '').replace('null', 'NaN').split(',')[1:]
        r5 = r4 + r44
        crypto_currs_answer = 1
    except:
        crypto_currs_answer = 0
    
    finally:
        data = dict(zip(crypts,r5))
        data['neo'] = 0
    return data


def get_all_comissions():
    try:
        com_usd = full_cryptocurrs_data('transactionfees')

        coms = requests.get('https://api.coinmarketcap.com/v1/ticker/').json()
        
        btc_usd = float(coms[0]['price_usd'])
        eth_usd = float(coms[1]['price_usd'])
        rub_usd = float(c.get_rates('USD')['RUB'])
        eur_usd = float(c.get_rates('USD')['EUR'])

        com_btc = {}
        com_eth = {}
        com_eur = {}
        com_rub = {}
        comissions_answer = 1
    except:
        comissions_answer = 0
    finally:
        for key in com_usd:
            com_btc[key] = float(com_usd[key])/btc_usd
            com_eth[key] = float(com_usd[key])/eth_usd
            com_eur[key] = float(com_usd[key])*eur_usd
            com_rub[key] = float(com_usd[key])*rub_usd
    return com_usd,com_btc,com_eth,com_eur,com_rub