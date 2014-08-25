Fine Grained Routing and Node Placement on Titan
====

To generate a placement strategy for *atlas1* only, with 1008 MPI ranks, but
minus failed compute node, 4, 8, 18:

    $ ./fgr.py placement --paritition atlas1 --numranks 1008 --failed 4 8 18

To generate a placement strategy for both *atlas1* and *atlas2*, with 2016 MPI
ranks:

    $ ./fgr.py placement --partition atlas --numranks 2016

To generate a random placement strategy for *atlas2* only:

    $ ./fgr.py placement --partition atlas2 --strategy random --numranks 1008


