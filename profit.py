import psycopg2
import psycopg2.extras

# нужны плавающие курсы
usd_rub = 57
usd_eur = 0.83 
btc_usd = 16500 
bch_usd = 2740 
dsh_usd = 1270 
zec_usd = 615

btc_com_usd = 32.515
btc_com_rub = btc_com_usd * usd_rub
btc_com_eur = btc_com_usd * usd_eur
eth_com_usd = 2.452
eth_com_eur = eth_com_usd*usd_eur
eth_com_btc = eth_com_usd/btc_usd
dsh_com_usd = 0.637
dsh_com_eur = dsh_com_usd * usd_eur
dsh_com_btc = dsh_com_usd/dsh_usd
zec_com_usd = 0.03
zec_com_btc = zec_com_usd/zec_usd
bch_com_usd = 0.219
bch_com_btc = bch_com_usd/bch_usd
bch_com_eur = bch_com_usd*usd_eur

conn_string = "dbname='igor' user='server' password='Chordify2811' host='138.197.179.83'"
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
    letter1 = wex_cex_data[i][0][0:3]
    letter2 = wex_cex_data[i][0][4:7]
    if letter1 == 'btc' and letter2 == 'usd':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -btc_com_usd)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'btc' and letter2 == 'rub':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -btc_com_rub)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'btc' and letter2 == 'eur':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -btc_com_eur)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'eth' and letter2 == 'usd':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -eth_com_usd)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'eth' and letter2 == 'btc':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -eth_com_btc)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'eth' and letter2 == 'eur':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -eth_com_eur)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'bch' and letter2 == 'usd':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -bch_com_usd)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'bch' and letter2 == 'btc':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -bch_com_btc)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'bch' and letter2 == 'eur':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -bch_com_eur)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'dsh' and letter2 == 'usd':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -dsh_com_usd)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'dsh' and letter2 == 'btc':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -dsh_com_btc)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'dsh' and letter2 == 'eur':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -dsh_com_eur)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'zec' and letter2 == 'usd':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -zec_com_usd)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)
    elif letter1 == 'zec' and letter2 == 'btc':
        wex_cex_results.append((((float(wex_cex_data[i][2]) * wex_com * wex_out_com -zec_com_btc)* cex_com) / float(wex_cex_data[i][1]) - 1) * 100)

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
