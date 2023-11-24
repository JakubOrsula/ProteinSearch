import python_distance
import gemmi

protein = "/mnt/data/PDBe_raw/xh/2xhc.cif"
protein = '/mnt/data/PDBe_raw/as/1asj.cif'
protein = '/mnt/data/PDBe_raw/pc/3pcc.cif'

protein_out = '/tmp/stripped.bin'


def strip_file(filename):
    with open(filename, 'r') as fin:
        contents = fin.read()
        doc = gemmi.cif.read_string(contents)
        block = doc.sole_block()
        pdb_id = block.find_pair('_struct.entry_id')
        pdb_title = block.find_pair('_struct.title')

        lines = []

        for line in contents.splitlines(keepends=True):
            if line.startswith(('data_', 'loop_', '_atom_site', 'ATOM ', 'HETATM ', '#')):
                lines.append(line)
        for i, line in enumerate(lines[:-1]):
            if line.startswith('loop_') and not lines[i + 1].startswith('_atom_site.group_PDB'):
                lines[i] = None
        lines = [line for line in lines if line is not None]
        for i in range(1, len(lines)):
            if lines[i].startswith('#') and lines[i - 1].startswith('#'):
                lines[i - 1] = None
        lines = [line for line in lines if line is not None]

    lines += (f"_struct.entry_id {pdb_id}\n", '#\n' + f"_struct.title {pdb_title}\n", '#\n')
    return lines

with open(protein_out, 'w') as fout:
        fout.writelines(strip_file(protein))



print(python_distance.save_chains(protein_out, '/tmp', 'test'))


def get_raw_from_gesamt(strid):
    gesamt = strid.split(':')[0]
    dirname = strid[1:3]

    return f"{dirname}/{gesamt}"
