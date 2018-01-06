import psycopg2
conn = psycopg2.connect(dbname="igor", user="bot", password="Chordify2811", host="138.197.179.83")
cur = conn.cursor()
cur.execute("SELECT br, value FROM pairs ORDER BY idt DESC LIMIT 14;")
profit = cur.fetchall()
cur.close()
conn.close()
