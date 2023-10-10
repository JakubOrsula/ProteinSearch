import python_distance

THRESHOLD=0.6

for pivot in ["2L1Q:A", "3UP2:A", "5YRA:A"]:
    o_p = python_distance.get_results('101M:A', pivot, '/mnt/PDBe_clone_binary', THRESHOLD)
    p_o = python_distance.get_results(pivot, '101M:A', '/mnt/PDBe_clone_binary', THRESHOLD)
    assert o_p[1] - p_o[1] == 0, (o_p[1], p_o[1])
    print(pivot, p_o[1], o_p[1])