
## Fine Grained Routing and Node Placement on Titan

To generate a placement strategy for *atlas1* only, with 1008 MPI ranks, but
minus failed compute node, 4, 8, 18:

    $ ./fgr2.py placement --paritition atlas1 --numranks 1008 --failed 4 8 18

To generate a placement strategy for both *atlas1* and *atlas2*, with 2016 MPI
ranks:

    $ ./fgr2.py placement --partition atlas --numranks 2016

To generate a random placement strategy for *atlas2* only:

    $ ./fgr2.py placement --partition atlas2 --strategy random --numranks 1008


Upon execution, two output are generated: a shell scripts that is designed to
run on Titan to realize the particular placement scheme; and a debug output
that aims to provide the context/debug information on why the chosen node.
