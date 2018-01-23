import telebot
from telebot import types
import requests
import pandas as pd
import logging
import psycopg2
import cherrypy

token = '513646383:AAF7ZrrSmwjH9SA2F8o5cH4z7iwWgXlhnEo'
bot = telebot.TeleBot(token)


@bot.message_handler(commands=['start'])
def start(m):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(*[types.KeyboardButton(name) for name in ['Обновить данные (лучшие)','Обновить данные (все)', 'Список бирж']])
    msg = bot.send_message(m.chat.id, 'Добро пожаловать! \nБот ищет возможности для арбитратража криптовалютами с учетом комиссий. \n*Важно* : бот не учитывает комиссии на вывод средств в фиат',parse_mode='Markdown', reply_markup=keyboard)
    bot.register_next_step_handler(msg, name)

def name(m):
    if m.text == 'Обновить данные (лучшие)':

        conn_string = "dbname='igor'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("SELECT id FROM pairs_ts ORDER BY id DESC LIMIT 1;")
        idt = cur.fetchone()
        cur.execute("SELECT fex, sex, br, delta FROM pairs WHERE idt=%s AND delta > 1 ORDER BY delta DESC LIMIT 10", idt) #Обновить данные (лучшие)
        data=cur.fetchall()
        cur.close()
        conn.close()

        exchange_first = []
        exchange_second = []
        pair = []
        delta = []

        for i in data:
            exchange_first.append(i[0])
            exchange_second.append(i[1])
            pair.append(i[2])
            delta.append(i[3])
        currencies = {'exc1': exchange_first, 'exc2': exchange_second, 'pair': pair, 'delta': delta}
        currencies = pd.DataFrame(currencies)
        cols = ['exc1', 'exc2', 'pair', 'delta']
        currencies = currencies[cols]
        msg = bot.send_message(m.chat.id, '{}'.format(currencies.to_string(index=False)))

        bot.register_next_step_handler(msg, name)

    if m.text == 'Обновить данные (все)':
        
        conn_string = "dbname='igor'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("SELECT id FROM pairs_ts ORDER BY id DESC LIMIT 1;")
        idt = cur.fetchone()
        cur.execute("SELECT fex, sex, br, delta FROM pairs WHERE idt=%s AND delta > 1 ORDER BY delta DESC", idt) # Обновить данные (все)
        data=cur.fetchall()
        cur.close()
        conn.close()

        exchange_first = []
        exchange_second = []
        pair = []
        delta = []

        for i in data:
            exchange_first.append(i[0])
            exchange_second.append(i[1])
            pair.append(i[2])
            delta.append(i[3])
        currencies = {'exc1': exchange_first, 'exc2': exchange_second, 'pair': pair, 'delta': delta}
        currencies = pd.DataFrame(currencies)
        cols = ['exc1', 'exc2', 'pair', 'delta']
        currencies = currencies[cols]

        msg = bot.send_message(m.chat.id, '{}'.format(currencies.head(50).to_string(index=False)))

        bot.register_next_step_handler(msg, name)

    if m.text == 'Список бирж':
        msg = bot.send_message(m.chat.id, 'Binance\nBitfinex\nBittrex\nCex\nPoloniex\nWex',parse_mode='Markdown')

        bot.register_next_step_handler(msg, name)



if __name__ == '__main__':
    logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s', level=logging.DEBUG, filename=u'mylog.log')
    bot.polling(none_stop=True)


