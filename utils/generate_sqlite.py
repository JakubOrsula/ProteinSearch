import sqlite3


conn = sqlite3.connect('../app/chain_ids.db')
c = conn.cursor()
c.execute('CREATE TABLE chain_ids (messif_id INTEGER PRIMARY KEY, chain_id TEXT);')

with open('Proteins_Global_ID_map.csv') as f:
    for line in f:
        chain_id, messif_id = line.strip().split(';')
        c.execute(f'INSERT INTO chain_ids VALUES({messif_id}, "{chain_id}")')

conn.commit()
conn.close()
