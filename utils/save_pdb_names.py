import mariadb
import sys
import os
import gemmi

DB_HOST = 'localhost'
DB_NAME = 'protein_chain_db'
DB_USER = 'chain'
DB_PASS = 'OneCha1n2RuleThem4ll'


def main():
    directory = sys.argv[1]

    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()

    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_file():
                doc = gemmi.cif.read_file(entry.path)
                block = doc.sole_block()
                pdb_id = block.find_pair('_struct.entry_id')
                pdb_title = block.find_pair('_struct.title')

                if pdb_id is None or pdb_title is None:
                    print(f'Error: No info found for: {entry.path}')
                else:
                    pdb_id = pdb_id[1]
                    delimiter = pdb_title[1][0]
                    title = pdb_title[1].strip(delimiter).strip()
                    print(pdb_id, title)
                    insert_query = f'INSERT INTO protein VALUES (%s, %s)'
                    c.execute(insert_query, (pdb_id, title))

    conn.commit()
    c.close()
    conn.close()


if __name__ == '__main__':
    main()
