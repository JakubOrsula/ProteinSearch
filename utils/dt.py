import python_distance

q = '101M:A'
threshold = 0.0

for pivot in ["2L1Q:A", "3UP2:A", "5YRA:A"]:
    print(pivot)
    res1 = python_distance.get_results(q, pivot, '/mnt/PDBe_clone_binary', threshold)
    print(res1)
    res2 = python_distance.get_results(pivot, q, '/mnt/PDBe_clone_binary', threshold)
    print(res2)
