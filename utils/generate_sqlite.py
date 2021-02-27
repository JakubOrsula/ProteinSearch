import sqlite3


conn = sqlite3.connect('../app/chain_ids.db')
c = conn.cursor()
c.execute('CREATE TABLE indexed_ids (messif_id INTEGER PRIMARY KEY AUTOINCREMENT, chain_id TEXT);')
c.execute('CREATE TABLE uploaded_ids (messif_id INTEGER PRIMARY KEY AUTOINCREMENT, chain_id TEXT);')
c.execute('INSERT INTO SQLITE_SEQUENCE VALUES ("uploaded_ids", 1000000);')
c.execute('''CREATE VIEW chain_ids AS
    SELECT messif_id -1 as messif_id, chain_id from indexed_ids
    UNION ALL
    SELECT messif_id, chain_id from uploaded_ids;''')

with open('Proteins_Global_ID_map.csv') as f:
    for line in f:
        chain_id, _ = line.strip().split(';')
        c.execute(f'INSERT INTO indexed_ids VALUES(NULL, "{chain_id}")')

conn.commit()
conn.close()
