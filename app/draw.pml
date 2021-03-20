from sys import argv
set max_threads, 2
cmd.load(argv[2], 'aligned')
cmd.load(argv[1], 'query')
set_color query_color, [209, 182, 182]
set_color aligned_color, [125, 188, 252]
color query_color, query
color aligned_color, aligned
hide (het)
set ray_shadows, off
cmd.png(argv[3], 400, 400)
quit
