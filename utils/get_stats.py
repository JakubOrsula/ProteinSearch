import python_distance

protein = "/mnt/data-ssd/PDBe_raw/xh/2xhc.cif"

print(python_distance.save_chains(f'/mnt/data-ssd/PDBe_raw/{protein}.cif', '/tmp', 'test'))


def get_raw_from_gesamt(strid):
    gesamt = strid.split(':')[0]
    dirname = strid[1:3]

    return f"{dirname}/{gesamt}"
