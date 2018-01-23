import telebot
from telebot import types
import requests
import pandas as pd
import logging
import psycopg2
import cherrypy

token = '513646383:AAF7ZrrSmwjH9SA2F8o5cH4z7iwWgXlhnEo'
WEBHOOK_HOST = '138.197.179.83'
WEBHOOK_PORT = 8443  # 443, 80, 88 или 8443 (порт должен быть открыт!)
WEBHOOK_LISTEN = '138.197.179.83'  # На некоторых серверах придется указывать такой же IP, что и выше

WEBHOOK_SSL_CERT = '../webhook_cert.pem'  # Путь к сертификату
WEBHOOK_SSL_PRIV = '../webhook_pkey.pem'  # Путь к приватному ключу

WEBHOOK_URL_BASE = "https://%s:%s" % (WEBHOOK_HOST, WEBHOOK_PORT)
WEBHOOK_URL_PATH = "/%s/" % (token)
bot = telebot.TeleBot(token)

class WebhookServer(object):
    @cherrypy.expose
    def index(self): 
        if 'content-length' in cherrypy.request.headers and \
                        'content-type' in cherrypy.request.headers and \
                        cherrypy.request.headers['content-type'] == 'application/json':
            length = int(cherrypy.request.headers['content-length'])
            json_string = cherrypy.request.body.read(length).decode("utf-8")
            update = telebot.types.Update.de_json(json_string)
            # Эта функция обеспечивает проверку входящего сообщения
            bot.process_new_updates([update])
            return ''
        else:
            raise cherrypy.HTTPError(403)

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

# Снимаем вебхук перед повторной установкой (избавляет от некоторых проблем)
bot.remove_webhook()

 # Ставим заново вебхук
bot.set_webhook(url=WEBHOOK_URL_BASE + WEBHOOK_URL_PATH,
                certificate=open(WEBHOOK_SSL_CERT, 'r'))

# Указываем настройки сервера CherryPy
cherrypy.config.update({
    'server.socket_host': WEBHOOK_LISTEN,
    'server.socket_port': WEBHOOK_PORT,
    'server.ssl_module': 'builtin',
    'server.ssl_certificate': WEBHOOK_SSL_CERT,
    'server.ssl_private_key': WEBHOOK_SSL_PRIV
})

 # Собственно, запуск!
cherrypy.quickstart(WebhookServer(), WEBHOOK_URL_PATH, {'/': {}})
