import python_distance

protein = "/mnt/data/PDBe_raw/xh/2xhc.cif"
protein = '/mnt/data/PDBe_raw/as/1asj.cif'
protein = '/mnt/data/PDBe_raw/pc/3pcc.cif'

protein_out = '/tmp/stripped.bin'

with open(protein, 'r') as fin:
    lines = []
    for line in fin:
        if line.startswith(('data_', '_entry', 'loop_', '_atom_site', 'ATOM ', 'HETATM ', '#')):
            lines.append(line)
    for i, line in enumerate(lines[:-1]):
        if line.startswith('loop_') and not lines[i+1].startswith('_atom_site.group_PDB'):
            lines[i] = None
    lines = [line for line in lines if line is not None]
    for i in range(1, len(lines)):
        if lines[i].startswith('#') and lines[i - 1].startswith('#'):
            lines[i-1] = None
    lines = [line for line in lines if line is not None]

with open(protein_out, 'w') as fout:
        fout.writelines(lines)



print(python_distance.save_chains(protein_out, '/tmp', 'test'))


def get_raw_from_gesamt(strid):
    gesamt = strid.split(':')[0]
    dirname = strid[1:3]

    return f"{dirname}/{gesamt}"
