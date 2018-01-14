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
cur.execute("SELECT wex.br, wex.value, cex.value, pol.value FROM wex INNER JOIN cex ON wex.br=cex.br INNER JOIN pol ON wex.br=pol.br WHERE wex.idt=(%s) and cex.idt=(%s) and pol.idt=(%s);", (idw, idc, idp,))
currency = cur.fetchall()
cur.close()
conn.close()
