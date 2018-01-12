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
cur.execute("SELECT wex.br, wex.value, cex.value FROM wex INNER JOIN cex ON cex.br=wex.br WHERE wex.idt=(%s) and cex.idt=(%s);", (idw, idc,))
wex_cex_data = cur.fetchall()
cur.execute("SELECT cex.br, cex.value, pol.value FROM cex INNER JOIN pol ON cex.br=pol.br WHERE cex.idt=(%s) and pol.idt=(%s);", (idc, idp,))
cex_pol_data = cur.fetchall()
cur.execute("SELECT wex.br, wex.value, pol.value FROM wex INNER JOIN pol ON wex.br=pol.br WHERE wex.idt=(%s) and pol.idt=(%s);", (idw, idp,))
wex_pol_data = cur.fetchall()
cur.execute("INSERT INTO pairs_ts(ts) VALUES (CURRENT_TIMESTAMP);")
conn.commit()
cur.execute("SELECT CURRVAL('pairs_ts_id_seq');")
pairs_id = cur.fetchone()
cur.close()
conn.close()
