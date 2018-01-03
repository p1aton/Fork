import telebot
from telebot import types
import requests
import pandas as pd


token = '528314642:AAH1NhBuOIpPcMFI-0CrglGt9q0E6s5PRGM'


bot = telebot.TeleBot(token)


@bot.message_handler(commands=['start'])
def start(m):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(*[types.KeyboardButton(name) for name in ['Обновить данные', 'Список бирж']])
    msg = bot.send_message(m.chat.id, 'Добро пожаловать', reply_markup=keyboard)
    bot.register_next_step_handler(msg, name)

def name(m):
    if m.text == 'Обновить данные':
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

        words = ['bch_bts', 'bch_eur', 'bch_usd', 'btc_eur', 'btc_usd', 'dsh_btc', 'dsh_eur', 'dsh_usd', 'eth_btc',
                 'eth_eur', 'eth_usd',
                 'zec_btc', 'zec_usd']

        bch_btc = round((cex_bch_btc / wex_bch_btc - 1) * 100, 2)
        bch_eur = round((cex_bch_eur / wex_bch_eur - 1) * 100, 2)
        bch_usd = round((cex_bch_usd / wex_bch_usd - 1) * 100, 2)
        btс_eur = round((cex_btc_eur / wex_btc_eur - 1) * 100, 2)
        btс_usd = round((cex_btc_usd / wex_btc_usd - 1) * 100, 2)
        dsh_btc = round((cex_dsh_btc / wex_dsh_btc - 1) * 100, 2)
        dsh_eur = round((cex_dsh_eur / wex_dsh_eur - 1) * 100, 2)
        dsh_usd = round((cex_dsh_usd / wex_dsh_usd - 1) * 100, 2)
        eth_btc = round((cex_eth_btc / wex_eth_btc - 1) * 100, 2)
        eth_eur = round((cex_eth_eur / wex_eth_eur - 1) * 100, 2)
        eth_usd = round((cex_eth_usd / wex_eth_usd - 1) * 100, 2)
        zec_btc = round((cex_zec_btc / wex_zec_btc - 1) * 100, 2)
        zec_usd = round((cex_zec_usd / wex_zec_usd - 1) * 100, 2)

        nums = [bch_btc, bch_eur, bch_usd, btс_eur, btс_usd, dsh_btc, dsh_eur, dsh_usd, eth_btc,eth_eur, eth_usd, zec_btc,
                zec_usd]
        curr = pd.DataFrame({'Currency': words, 'Delta in %': nums})

        msg = bot.send_message(m.chat.id,'{}'.format(curr.sort_values(by='Delta in %',ascending=False).to_string(index=False)))
        bot.register_next_step_handler(msg, name)
    if m.text == 'Список бирж':
        msg = bot.send_message(m.chat.id, 'Бот работает с биржами *wex* и *cex*',parse_mode='Markdown')
        bot.register_next_step_handler(msg, name)


'''
@bot.message_handler(content_types=["text"])
def repeat_all_messages(message): # Название функции не играет никакой роли, в принципе
    bot.send_message(message.chat.id, message.text)
'''

if __name__ == '__main__':
    bot.polling(none_stop=True)

