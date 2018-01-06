import psycopg2
import psycopg2.extras
conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
wex_com = 0.998
wex_out_com = 0.999
cex_com = 0.998

conn = psycopg2.connect(conn_string)
cur = conn.cursor()
cur.execute("SELECT idt FROM wex ORDER BY idt DESC LIMIT 1;")
idw = cur.fetchone()
cur.execute("SELECT idt FROM cex ORDER BY idt DESC LIMIT 1;")
idc = cur.fetchone()
cur.execute("SELECT wex.br, wex.value, cex.value, wex.idt, cex.idt FROM wex INNER JOIN cex ON cex.br=wex.br WHERE wex.idt=(%s) and cex.idt=(%s) LIMIT 14;", (idw, idc,))
wex_cex_data = cur.fetchall()
cur.execute("INSERT INTO pairs_ts(ts) VALUES (CURRENT_TIMESTAMP);")
conn.commit()
cur.execute("SELECT CURRVAL('pairs_ts_id_seq');")
pairs_id = cur.fetchone()
cur.close()
conn.close()
names = []
wex_cex_results = []
pairs = []
for i in range(0, len(wex_cex_data)):
    wex_cex_results.append(((float(wex_cex_data[i][2]) * wex_com * wex_out_com * cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
for i in range(0, len(wex_cex_data)):
    names.append(wex_cex_data[i][0])
for i in range(0,len(names)):
    pairs.append((names[i], wex_cex_results[i], pairs_id[0]))
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
psycopg2.extras.execute_values(cur, "INSERT INTO pairs(br, value, idt) values %s", pairs)
conn.commit()
cur.close()
conn.close()
