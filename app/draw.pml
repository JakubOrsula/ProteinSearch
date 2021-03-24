from sys import argv
set max_threads, 2
cmd.load(argv[2], 'aligned')
cmd.load(argv[1], 'query')
zoom complete=1
set_color query_color, [33, 155, 119]
set_color aligned_color, [192, 85, 25]
color query_color, query
color aligned_color, aligned
hide (het)
ray 400, 400
cmd.png(argv[3])
quit
