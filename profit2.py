import psycopg2
import psycopg2.extras
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
cur.execute("SELECT idt FROM wex ORDER BY idt DESC LIMIT 1;")
idw = cur.fetchone()
cur.execute("SELECT idt FROM cex ORDER BY idt DESC LIMIT 1;")
idc = cur.fetchone()
cur.execute("SELECT idt FROM pol ORDER BY idt DESC LIMIT 1;")
idp = cur.fetchone()
cur.execute("SELECT currencies.cur, wex.value, cex.value, pol.value FROM currencies FULL OUTER JOIN (SELECT * FROM wex WHERE wex.idt=(%s)) wex ON currencies.cur=wex.br FULL OUTER JOIN (SELECT * FROM cex WHERE cex.idt=(%s)) cex ON currencies.cur=cex.br FULL OUTER JOIN (SELECT * FROM pol WHERE pol.idt=(%s)) pol ON currencies.cur=pol.br;", (idw, idc, idp,))
data = cur.fetchall()
cur.close()
conn.close()
