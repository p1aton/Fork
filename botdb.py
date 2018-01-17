import psycopg2
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
cur.execute("SELECT id FROM pairs_ts ORDER BY id DESC LIMIT 1;")
idt = cur.fetchone()
cur.execute("SELECT fex, sex, br, fvalue, svalue, delta FROM pairs WHERE idt=%s", idt)
data = cur.fetchall()
cur.close()
conn.close()
