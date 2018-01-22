import psycopg2
conn_string = "dbname='igor' user='bot' password='Chordify2811' host='138.197.179.83'"
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
cur.execute("SELECT id FROM pairs_ts ORDER BY id DESC LIMIT 1;")
idt = cur.fetchone()
cur.execute("SELECT fex, sex, br, delta FROM pairs WHERE idt=%s AND delta > 1 ORDER BY delta DESC", idt) # Обновить данные (все)
data=cur.fetchall()
cur.close()
conn.close()

import psycopg2
conn_string = "dbname='igor' user='bot' password='Chordify2811' host='138.197.179.83'"
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
cur.execute("SELECT id FROM pairs_ts ORDER BY id DESC LIMIT 1;")
idt = cur.fetchone()
cur.execute("SELECT fex, sex, br, delta FROM pairs WHERE idt=%s AND delta > 1 ORDER BY delta DESC LIMIT 10", idt) #Обновить данные (лучшие)
data=cur.fetchall()
cur.close()
conn.close()
