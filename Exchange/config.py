conn_string = "dbname='igor' user='igor' password='Chordify2811' host='138.197.179.83'"
exchanges = ['wex', 'cex', 'pol', 'btfnx', 'binance', 'bittrex']
def write_ts(x):
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("INSERT INTO {}(ts) VALUES (CURRENT_TIMESTAMP);".format(x+'_ts'))
    cur.execute("SELECT CURRVAL('{}');".format(x+'_ts_id_seq'))
    idt = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return idt
def write_data(x, y):
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, "INSERT INTO {}(br, value, idt) values %s".format(x), y)
    conn.commit()
    cur.close()
    conn.close()
