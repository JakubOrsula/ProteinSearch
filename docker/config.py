import os

QUERIES_DIR = '/var/local/ProteinSearch'
COMPUTATIONS_DIR = os.path.join(QUERIES_DIR, 'computations')
ARCHIVE_DIR = '/data/PDBe_clone_binary'
RAW_PDB_DIR = '/data/PDBe_clone'
PRELOAD_LIST = '/data/pivots'
QSCORE_THRESHOLD = 0.1

DATABASE = None
DB_USER = None
DB_PASS = None
