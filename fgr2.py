#!/usr/bin/env python
"""
Client placement strategies based on Fine Grained Routing

@author: Feiyi Wang
@email:  fwang2@gmail.com
"""

import argparse
import random
import re
import logging
import time
import string
import sys
from datetime import datetime
from collections import defaultdict

# Globals

ARGS   = None
logger = None

rtrA = ["c7-2c2s0", "c23-2c1s7", "c10-2c0s0", "c3-6c0s2", "c19-6c2s2", "c14-6c1s5",
        "c7-5c2s5", "c23-5c1s2", "c10-5c0s5", "c3-1c1s4", "c19-1c0s3", "c14-1c2s3" ]

rtrB = ["c5-0c0s5", "c19-0c1s2", "c12-0c2s5", "c1-4c2s7", "c15-4c0s7", "c16-4c1s0",
        "c5-7c1s5", "c19-7c2s2", "c12-7c0s2", "c1-3c2s0", "c15-3c0s0", "c16-3c1s7"]

rtrC = ["c5-0c2s1", "c19-0c0s1", "c12-0c1s6", "c1-4c1s4", "c15-4c2s3", "c16-4c0s3",
        "c5-7c2s6", "c19-7c0s6", "c12-7c1s1", "c1-3c0s4", "c15-3c1s3", "c16-3c2s4" ]

rtrD = ["c13-0c1s5", "c22-0c2s2", "c4-0c0s2", "c9-4c0s4", "c23-4c1s3", "c8-4c2s4",
        "c13-7c1s0", "c22-7c2s7", "c4-7c0s7", "c9-3c2s5", "c23-3c0s5", "c8-3c1s2" ]

rtrE = ["c13-0c2s6", "c22-0c0s6", "c4-0c1s1", "c9-4c2s0", "c23-4c0s0", "c8-4c1s7",
        "c13-7c0s3", "c22-7c1s4", "c4-7c2s3", "c9-3c1s6", "c23-3c2s1", "c8-3c0s1" ]

rtrF = ["c3-2c1s0", "c21-2c0s7", "c14-2c2s7", "c0-6c1s6", "c17-6c0s1", "c18-6c2s1",
        "c3-5c0s4", "c21-5c2s4", "c14-5c1s3", "c0-1c2s2", "c17-1c1s5", "c18-1c0s2"]

rtrG = ["c3-2c2s3", "c21-2c1s4", "c14-2c0s3", "c0-6c0s5", "c17-6c2s5", "c18-6c1s2",
        "c3-5c1s7", "c21-5c0s0", "c14-5c2s0", "c0-1c1s1", "c17-1c0s6", "c18-1c2s6"]

rtrH = ["c11-2c0s6", "c20-2c2s6", "c6-2c1s1", "c7-6c0s0", "c24-6c2s0", "c10-6c1s7",
        "c11-5c2s3", "c20-5c1s4", "c6-5c0s3", "c7-1c1s6", "c24-1c0s1", "c10-1c2s1"]

rtrI = ["c11-2c1s5", "c20-2c0s2", "c6-2c2s2", "c7-6c2s4", "c24-6c1s3", "c10-6c0s4",
        "c11-5c1s0", "c20-5c0s7", "c6-5c2s7", "c7-1c0s5", "c24-1c2s5", "c10-1c1s2"]

rtrALL = [rtrA, rtrB, rtrC, rtrD, rtrE, rtrF, rtrG, rtrH, rtrI]

class G:

    ###### Routers

    RTR_LIST = rtrA + rtrB + rtrC + rtrD + rtrE + rtrF + rtrG + rtrH + rtrI  # all router modules
    RTR2LNET = {}  # Router name to LNET mapping


    ATLAS1_RTRS = [] # Routers associated with atlas1
    ATLAS2_RTRS = [] # Routers associated with atlas2
    RID2ROUTER = defaultdict() # router id -> router object

    # each router, client list ordered by costs
    # router nid -> { client list }
    #
    # populated by rtr_client_costs()
    RTR_CLIENTS = defaultdict(list)

    ####### Client

    CLIENTS = [] # all clients
    NID2X = {}
    NID2Y = {}
    NID2Z = {}
    NID2COL = {}
    NID2CNAME = {}

    # each client, each router, the cost
    # client_nid -> router nid -> cost
    CLIENT_COSTS = defaultdict(lambda: defaultdict(int))

    # each router, each cost, the clients
    # {rtr_id as key -> {cost -> [ ... clients ...]}
    ROUTER_COSTS = defaultdict(lambda: defaultdict(list))

    # hold currently selected client tuple
    # each tuple is (client, ost, rtr, lnet, cost)
    SELECTED_CLIENTS = []

    ####### LNETS

    LNET2OST = defaultdict(list) # lnet -> list of OSTs
    OST2LNET = defaultdict(int) # {0..2015} -> lnet

class Node:

    def __init__(self, cname):

        res = re.search(r"c(\d+)-(\d+)c(\d+)s(\d+)n?(\d+)?", cname)
        g = res.groups()

        self.col, self.row, self.cage, self.slot, self.n = map(int, [g[0], g[1], g[2], g[3], g[4]])
        self.cname = cname

    def __str__(self):
        return self.cname

    def name(self):
        return self.__str__()

class Router:
    def __init__(self, nid, cname, interface, x, y, z):
        self.nid = nid
        self.cname = cname
        self.interface = interface
        self.x = x
        self.y = y
        self.z = z

        self.partition = None
        if interface in ["n0", "n2"]:
            self.partition = "atlas1"
            G.ATLAS1_RTRS.append(self)
        elif interface in ["n1", "n3"]:
            self.partition = "atlas2"
            G.ATLAS2_RTRS.append(self)
        else:
            logger.critical("Unrecognized interface %s", interface)
            sys.exit(1)

        # work out switch/lnet mapping
        hname = cname + interface
        self.lnet = G.RTR2LNET[hname]

    def __str__(self):
        return "%s %s %s%s %s %s %s" % (self.partition, self.lnet, self.cname, self.interface, self.x, self.y, self.z)

    def info(self):
        return self.__str__()


def str2node(s):
    """
    Convert a string such as c0-2c1s7n0 to Node.
    A router module doesn't have n{0..3}
    """
    res = re.search(r"c(\d+)-(\d+)c(\d+)s(\d+)n?(\d+)?", s)
    g = res.groups()
    return Node(g[0], g[1], g[2], g[3], g[4])

def parse_args():
    parser = argparse.ArgumentParser(description="FGR Program")

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("-v", "--verbose", default=False, action="store_true", help="verbose output")
    parent_parser.add_argument("--failed", type=int, nargs="+", help="A list of failed computes")
    parent_parser.add_argument("--map", default="titan.map", help="Titan map filename")
    parent_parser.add_argument("--username", default="fwang2", help="Provide user name")
    parent_parser.add_argument("--iorbin", default="/lustre/atlas2/test/fwang2/iotests/ior-test/IOR.posix", help="IOR bin")
    parent_parser.add_argument("--fgrfile", default="routing.map", help="Routing map")
    parent_parser.add_argument("--nodefile",  help="Node list")
    subparsers = parser.add_subparsers(help="Provide one of the sub-commands")


    mapinfo_parser = subparsers.add_parser("mapinfo", parents=[parent_parser], help="Generate various map")
    mapinfo_parser.set_defaults(func=main_mapinfo)


    placement_parser = subparsers.add_parser("placement", parents=[parent_parser], help="Generate placement")
    placement_parser.add_argument("--numranks", type=int, default=1008, help="num of ranks")
    placement_parser.add_argument("--partition", choices=['atlas1', 'atlas2', 'atlas'],
                                  default='atlas2', help="Select partition type")
    placement_parser.add_argument("--strategy", choices=["random", "hybrid"], default="hybrid", help="Placement type")
    placement_parser.set_defaults(func=main_placement)

    myargs = parser.parse_args()
    return myargs

def dist(v1, v2, dim):
    if v1 == v2: return 0
    d = None
    if v2 < v1:
        v1, v2 = v2, v1
    d = v2 - v1
    if d > int(dim/2):
        v1 += dim
        d = v1 - v2
    return d

def fgr_prepare():
    """
    pre-processing
    """

    with open(ARGS.map, "r") as f:
        for line in f:
            nid, cname, nodetype, x, y, z = line.split()
            nid, x, y, z = map(int, [nid, x, y, z])

            if nodetype == "compute":
                G.CLIENTS.append(nid)

            G.NID2CNAME[nid] = cname
            G.NID2X[nid], G.NID2Y[nid], G.NID2Z[nid] = x, y, z

            create_rtr_list(cname, nid, x, y, z)

    if ARGS.nodefile:
        G.CLIENTS = []
        try:
            with open(ARGS.nodefile, "r") as f:
                for line in f:
                    nid = int(line.strip())
                    G.CLIENTS.append(nid)
        except IOError, e:
            print("Read %s error: \n %s" % (ARGS.nodefile, e))
            sys.exit(1)


    if ARGS.failed:
        G.CLIENTS = list(set(G.CLIENTS) - set(ARGS.failed))

    logger.info("G.CLIENTS contains [%s] nids", len(G.CLIENTS))


    with open(ARGS.fgrfile, "r") as f:
        for line in f:
            entry = line.split()
            nid = int(entry[0])
            if nid not in G.CLIENTS: continue
            rtrlist = entry[1:]
            for ele in rtrlist:
                # ele takes the form of o2ib201:17736
                # ele[4:] will cut away o2ib, with 201:17736 left
                # we split and convert it to integer value
                lnet, rtr = [ int(i) for i in ele[4:].split(":")]
                # G.ROUTERS.add(rtr)
                #if rtr not in G.LNET_ROUTERS[lnet]:
                #    G.LNET_ROUTERS[lnet].append(rtr)

                cost = 4 * dist(G.NID2X[nid], G.NID2X[rtr], 25)
                cost += 8 * dist(G.NID2Y[nid], G.NID2Y[rtr], 16)
                cost += dist(G.NID2Z[nid], G.NID2Z[rtr], 24)
                cost += 100 # TODO: for verification only

                G.ROUTER_COSTS[rtr][cost].append(nid)
                G.CLIENT_COSTS[nid][rtr] = cost


    for ost in range(1008):
        base = int((ost % 144) / 72) * 9
        offset = int((ost + 4) / 8) % 9
        lnet = 201 + base + offset
        G.LNET2OST[lnet].append(ost)
        G.OST2LNET[ost] = lnet

        # atlas2
        ost2 = ost + 1008
        lnet += 18
        G.LNET2OST[lnet].append(ost2)
        G.OST2LNET[ost2] = lnet

    # for each router, we sort clients based on cost

    for rtr in G.ROUTER_COSTS.keys():
        s_costs = sorted(G.ROUTER_COSTS[rtr].keys()) # sorted
        for cost in s_costs:
            G.RTR_CLIENTS[rtr] += G.ROUTER_COSTS[rtr][cost]

def create_rtr_list(cname, nid, x, y, z):
    """
    populate G.ATLAS1_RTRS and G.ATLAS2_RTRS with Router objects
    """

    # candiate, substring except the last 2 chars
    # interface, the last 2 chars

    candidate = cname[0:-2]
    interface = cname[-2:]
    if candidate in G.RTR_LIST:
        r = Router(nid, candidate, interface, x, y, z)
        G.RID2ROUTER[nid] = r

def main_mapinfo():
    fgr_prepare()
    with open("lnet2ost.map", "w") as f:
        for lnet in range(201, 237):
            osts = " ".join([str(i) for i in G.LNET2OST[lnet]])
            f.write("%s %s\n\n" % (lnet, osts))
    logger.info("Generate lnet2ost.map");

    with open("ost2lnet.map", "w") as f:
        for k, v in G.OST2LNET.iteritems():
            f.write("%s %s\n" % (k,v))
    logger.info("Generate ost2lnet.map")


    with open("atlas1-rtr.map", "w") as f:
        for t in G.ATLAS1_RTRS:
                f.write(t.info() + "\n")

    with open("atlas2-rtr.map", "w") as f:
        for t in G.ATLAS2_RTRS:
                f.write(t.info() + "\n")

    logger.info("Generate atlas1-rtr.map")
    logger.info("Generate atlas2-rtr.map")

    # G.RTR_CLIENTS
    with open("rtr2client.map", "w") as f:
        for rtr in G.RTR_CLIENTS.keys():
            f.write("%s %s\n" % (rtr, len(G.RTR_CLIENTS[rtr])))

def best_client(rtr):
    """
    @param rtr:  a Router object
    @return: A tuple of (selected_client, cost)
    """

    rtr_nid = rtr.nid
    client = None

    while True:
        client = G.RTR_CLIENTS[rtr_nid][0]
        G.RTR_CLIENTS[rtr_nid].pop(0)
        if client in G.SELECTED_CLIENTS:
            continue
        else:
            break

    return client, G.CLIENT_COSTS[client][rtr_nid]

def select_client_hybrid(rtrs, numranks):
    # build up a list of all eligible OSTs
    # given the eligible routers
    logger.info("Eligible RTRs: %s", len(rtrs))

    while len(G.SELECTED_CLIENTS) < numranks:
        for rtr in rtrs:
            client, cost = best_client(rtr)
            picked_ost = G.LNET2OST[rtr.lnet].pop(0)
            G.LNET2OST[rtr.lnet].append(picked_ost)
            G.SELECTED_CLIENTS.append((client, picked_ost, cost, rtr.lnet, rtr))

    logger.info("Selected clients: %s", len(G.SELECTED_CLIENTS))

def timestamp():
    ts = time.time()
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d.%H%M%S')

def debug_hybrid(ofile):
    logger.info("Writing out %s", ofile)

    rtr2clients = defaultdict(list)

    for entry in G.SELECTED_CLIENTS:
        client, ost, cost, lnet, rtr = entry
        rtr2clients[rtr.nid].append(client)

    with open(ofile, "w") as f:
        for rtr in rtr2clients.keys():
            rtrobj = G.RID2ROUTER[rtr]
            clients = rtr2clients[rtr]
            f.write("Router %s: (%s, %s, %s)\n" % (rtr, rtrobj.x, rtrobj.y, rtrobj.z))
            for c in clients:
                f.write("\t Client: %s: (%s, %s, %s), cost=%s\n"
                        %(c, G.NID2X[c], G.NID2Y[c], G.NID2Z[c], G.CLIENT_COSTS[c][rtr]))
        f.close()

def current_opath(rtr, ts):
    if ARGS.partition == "atlas":
        if (rtr.partition == "atlas1"):
            return "/lustre/atlas1/test/%s/%s" % (ARGS.username, ts)
        elif rtr.partition == "atlas2":
            return "/lustre/atlas2/test/%s/%s" % (ARGS.username, ts)
        else:
            logger.critical("Uknown partition: %s", rtr.partition)
    else:
        return "/lustre/%s/test/%s/%s" % (ARGS.partition, ARGS.username, ts)


def gen_lfs_setstripe(fh, ts):
    """
    @param fh: an open file handle
    @param ts: timestamp
    """
    clients = []
    for idx, entry in enumerate(G.SELECTED_CLIENTS[:ARGS.numranks]):
        fname = "file." + string.rjust(str(idx), 8, "0")
        client, ost, cost, lnet, rtr = entry
        ost = ost % 1008
        opath_mkdir = current_opath(rtr, ts)
        clients.append(str(client))
        fh.write("lfs setstripe -c 1 -i %s %s/%s\n"  % (ost, opath_mkdir, fname))

    return clients

def gen_shell(ofile, clients = None):
    logger.info("Writing out %s", ofile)
    ts = timestamp()
    opath_ior = None  # path for IOR command -o

    if ARGS.partition != "atlas":
        opath_ior = "/lustre/%s/test/%s/%s/file" % (ARGS.partition, ARGS.username, ts)
    else:
        opath_ior = "/lustre/atlas1/test/%s/%s/file@/lustre/atlas2/test/%s/%s/file" % \
            (ARGS.username, ts, ARGS.username, ts)

    with open(ofile, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("#PBS -N ior-%s-placement\n" % ARGS.partition)
        f.write("#PBS -j oe\n")
        f.write("#PBS -q batch\n")
        f.write("#PBS -V\n")
        f.write("#PBS -A STF008\n")
        f.write("#PBS -l walltime=01:00:00\n")
        f.write("#PBS -l nodes=18688\n")
        f.write('[[ "$PBS_JOBID" ]] || PBS_JOBID=$(date +%s)\n')
        f.write('[[ "$PBS_O_WORKDIR" ]] && cd $PBS_O_WORKDIR\n')
        if ARGS.partition != "atlas":
            iopath = "/lustre/%s/test/%s/%s" % (ARGS.partition, ARGS.username, ts)
            f.write("rm -rf %s\n" % iopath)
            f.write("mkdir -p %s\n" % iopath)
        else:
            iopath1 = "/lustre/atlas1/test/%s/%s" % (ARGS.username, ts)
            iopath2 = "/lustre/atlas2/test/%s/%s" % (ARGS.username, ts)
            f.write("rm -rf %s\n" % iopath1)
            f.write("rm -rf %s\n" % iopath2)
            f.write("mkdir -p %s\n" % iopath1)
            f.write("mkdir -p %s\n" % iopath2)

        if ARGS.strategy == "hybrid":
            clients = gen_lfs_setstripe(f, ts)

        f.write("aprun -n %s -N 1 -L %s %s -a POSIX -b 32g -e -E -F -i 1 -k -t 1m -vv -w -D 30 -o %s\n"
                % (ARGS.numranks, ",".join(clients), ARGS.iorbin, opath_ior))
        f.close()

def gen_rtr2lnet():
    global rtrALL
    LNET_BASE = 201
    for i, rtrgroup in enumerate(rtrALL):
        for j, rtr in enumerate(rtrgroup):
            step = i % 9
            G.RTR2LNET[rtr + 'n0'] = LNET_BASE + step
            G.RTR2LNET[rtr + 'n2'] = LNET_BASE + 9 + step
            G.RTR2LNET[rtr + 'n1'] = LNET_BASE + 9 * 2 + step
            G.RTR2LNET[rtr + 'n3'] = LNET_BASE + 9 * 3 + step


def placement_random():
    random.seed()   # system time as seeds
    clients = map(str, random.sample(G.CLIENTS, ARGS.numranks))
    ofile = "%s_%s_%s.sh" % (ARGS.partition, ARGS.strategy, ARGS.numranks)
    gen_shell(ofile, clients)

def placement_hybrid():
    if ARGS.partition == "atlas1":
        select_client_hybrid(G.ATLAS1_RTRS, ARGS.numranks)
    elif ARGS.partition == "atlas2":
        select_client_hybrid(G.ATLAS2_RTRS, ARGS.numranks)
    elif ARGS.partition == "atlas":
        select_client_hybrid(G.ATLAS1_RTRS + G.ATLAS2_RTRS, ARGS.numranks)
    else:
        logger.critical("Unknown partition: %s", ARGS.partition)
        sys.exit(1)

    # client selection is done
    gen_shell("%s_%s_%s.sh" % (ARGS.partition, ARGS.strategy, ARGS.numranks))

    # debug output
    debug_hybrid("%s_%s.debug" % (ARGS.partition, ARGS.numranks))

def main_placement():
    fgr_prepare()
    if ARGS.strategy == "hybrid":
        placement_hybrid()
    elif ARGS.strategy == "random":
        placement_random()
    else:
        raise "Shouldn't happen"

def setup_logging(loglevel):
    global logger

    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, loglevel.upper()))
    fmt = logging.Formatter('%(levelname)s - %(message)s')
    fh = logging.FileHandler(filename="fgr.log", mode="w")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler();
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)

def main():

    global ARGS

    ARGS = parse_args()

    if ARGS.verbose:
        setup_logging("debug")
    else:
        setup_logging("info")

    logger.debug(ARGS)

    gen_rtr2lnet()

    try:
        ARGS.func()
    except KeyboardInterrupt:
        logger.info("Interrupted upon user request")
        sys.exit(1)

if __name__ == "__main__": main()
