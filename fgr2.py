#!/usr/bin/env python
"""
    Fine Grained Routing and client placement strategies

Usage
===========

(1) Generate placement for "atlas2" with 1008 ranks

    ./fgr.py placement --partition atlas2 --numranks 1008 > placement.1008

    you can skip both --partition and --numranks, the preset default is 1008
    ranks for atlas2

    The generated placement map's fields are defined as:

    #1 rank id
    #2 client id (nid)
    #3 ost index (for atlas2, % 1008)
    #4 LNET
    #5 router id
    #6 cost

(2) Generate various map (more for debugging than anything else)

    ./fgr mapinfo

    Currently, this will generate the following files:

    - routers.map, which is simply a list of all router ids

    - lnet2ost.map, this is a map between a LNET to all the OSTs it can reach

    - ost2lnet.map, this is a 1-1 mapping (one OST have a corresponding lnet)

(3) Generate more debug information

    After you have the placement file generated as (1). You can use the following
    command to print out more information, again, mostly for debugging or verification
    purpose.

    ./fgr debuginfo --placefile placment.1008

    The print out looks like the following

    1007 4070 2015@o2ib228 cost 102 nid_xyz (20, 11, 19) => router_xyz (20, 11,21) (4075)

    In the order of:

    rank_id  client_nid, ost_index@LNET cost # nid (x, y, z) ==> router (x, y, z) (router id)


(4) Verify Matt's placment

    ./conv_matt.py > placement.matt
    ./fgr.py debuginfo --placefile placement.matt

    56 clients for LNET 219

(5) Verify LNET to OST mapping

    this is specified through atlas1.conf atlas2.conf and spider-lnets.map

    FGR does its own calculation for this mapping, and through ./fgr mapinfo
    this information is written to a disk file

    verify_atlas_ost.py check the consistency of configuration files from Atlas
    and FGR-calculated results.

    ./verify_atlas_ost.py atlas1.conf
    ./verify_atlas_ost.py atlas2.conf

    Expect both to return "OKAY", and I don't expect this test
    needs to be done except debugging


Titan Physical layout
=====================

    c0-0    c1-0    c2-0 ... c24-0      ## row0
    c0-1    c1-1    c2-1 ... c24-1      ## row1
    c0-2    c1-2    c2-2 ... c24-2      ## row2
    c0-3    c1-3    c2-3 ... c24-3      ## row3
    ...
    c0-7    c1-7    c2-7 ... c24-7      ## row7



Compute node naming:
====================

    c23-5c1s2n2

    c23 - cabinet 23
    5   - row 5
    c1  - chasis 1
    s2  - slot 2
    n2  - compute node 2 (0..3)

If this is XIO blade, then the 4 I/O nodes are also named as n{0..3}.
That means, you can't tell them apart by just looking at the name.

Gemini port naming:
====================

    Say c0-0c0s0n0 belongs to same Gemini port, c0-0c0s0g0
        c0-0c0s0n1

    And c0-0c0s0n2 belongs to same Gemini port, c0-0c0s0g1
        c0-0c0s0n3

3D Torus
==========

The X dimension goes along rows; the Y dimension goes along columns, and Z
dimension goes between modules/blades within a cabinet.

The three dimension is limitted by:

(X = [0,24], Y= [0-15], Z = [0 -23])

Misc Notes
==========


At boot time, each compute node will run "node2route" logic and acquired
information on to reach a o2ib lnet, which:
        <o2ib lnet> <router NID> <gni lnet>
to use.

Also, the OSS node needs the map that tells for the particular o2ib LNET it
connects, <gni lnet 101>:<priority>:<router NID>
          <gni lnet 102>:<priority>:<router NID>
          ...
          <gni lnet 112>:<priority>:router NID>

This information is used for inbound traffic to Torus network, and which router
to pick.

This FGR program require python 2.7 - so it doesn't run with stock version of
RHEL or CentOS 6.x. If we do need this compatibility in the future, one
particular change required is dictionary comprehension:

    d = dict((key, value) for (key, value) in sequence)

"""

__author__ = "Feiyi Wang"
__email__  = "fwang2@gmail.com"


import argparse
import random
import re
import logging
import time
import string
import sys
import os
import cPickle as pickle
import subprocess
import operator
import multiprocessing

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

def dd_int():
    return defaultdict(int)

class G:
    """
    Misc global settings
    """

    BASE_GNI = 100
    BASE_O2IB = 201
    BASE_LNET = 201
    MESH_BIAS = 24

    CNAME = None  # used by "nodeinfo" for comparison

    # hold mapping from LNET to ROUTER, NID, the GNI
    LNET2RTR = {}
    LNET2NID = {}
    LNET2GNI = {}

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
    CNAME2NID = {}

    # client_nid -> router nid -> cost
    # = defaultdict(lambda: defaultdict(int))
    # would be more clear, but can't pickle it
    CLIENT_COSTS = defaultdict(dd_int)

    # each router, each cost, the clients
    # {rtr_id as key -> {cost -> [ ... clients ...]}
    ROUTER_COSTS = defaultdict(lambda: defaultdict(list))

    # hold currently selected client tuple
    # each tuple is (client, ost, rtr, lnet, cost)
    SELECTED_CLIENTS = []

    # client ID only, for check of duplicate
    SELECTED_CLIENT_IDS = []

    ####### LNETS

    LNET2OST = defaultdict(list) # lnet -> list of OSTs
    OST2LNET = defaultdict(int) # {0..2015} -> lnet

class Node:

    def __init__(self, cname):

        res = re.search(r"c(\d+)-(\d+)c(\d+)s(\d+)n?(\d+)?", cname)
        g = res.groups()

        self.col, self.row, self.cage, self.slot, self.n = map(int, [g[0], g[1], g[2], g[3], g[4]])
        self.nid, self.x, self.y, self.z = nodeinfo(cname)
        self.cname = cname

    def __str__(self):
        return self.cname

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


def dist_x(x1, x2):
    '''
    :param x1: node1
    :param x2: node2
    :return: x distrance based on TORUS
    '''

    v1 = (x1 - x2 + 25) % 25
    v2 = (x2 - x1 + 25) % 25
    if (v1 < v2):
        return v1
    else:
        return v2


def nsort(cname):
    """
    Given a compute node, cname,  this function
    returns distance of X-axis related to G.CNODE
    """
    node1 = str2node(cname)
    node2 = str2node(G.CNAME)
    return dist_x(node1.x, node2.x)

def sort_rtr3(rtr3):
    """
    rtr3 is a list of router modules belong to the selected sub-group,
    so there are 3 of them to be precise.

    This function will sort them based on which one is closer to
    compute node in terms of X-axis
    """

    rtr3 = [ x + "n0" for x in rtr3]
    rtr3.sort(key=nsort)
    return rtr3


def str2node(s):
    """
    Convert a string such as c0-2c1s7n0 to Node.
    A router module doesn't have n{0..3}
    """
    #res = re.search(r"c(\d+)-(\d+)c(\d+)s(\d+)n?(\d+)?", s)
    #g = res.groups()
    #return Node(g[0], g[1], g[2], g[3], g[4])

    return Node(s)


def nodeinfo_from_file(s):
    """
    Given a node name such as c0-2c1s7n0,
    return its NID, Cray's rca-helper can give this information
    we just lookup from provided map file
    """
    cmd = "grep %s %s" % (s, ARGS.map)
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    nid, nic, cname, gemini, x, y, z = stdout.split()

    return nid, int(x), int(y), int(z)

def nodeinfo(s):
    """
    When we parse the map, let's just save the mapping from cname to NID
    and save us from the trouble of read from file again like nodeinfo_from_file()
    :param s: cname
    :return: a tuple of (nid, x, y, z)
    """
    nid = G.CNAME2NID[s]
    return nid, G.NID2X[nid], G.NID2Y[nid], G.NID2Z[nid]

def nid(s):
    """
    A simplified version of nodeinfo() where both nid and its dimension info
    is returned, this function only returns its nid

    :param s: A node name such as c0-2c1s7n0
    :return: its numeric ID
    """
    nid, x, y, z = nodeinfo(s)
    return nid


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
    placement_parser.add_argument("--stripesize", default="1M", help="Set Lustre stripe size, default 1M")
    placement_parser.set_defaults(func=main_placement)

    nidinfo_parser = subparsers.add_parser("nidinfo", parents=[parent_parser], help="NID explorer")
    nidinfo_parser.add_argument("nid",type=int, help="A valid NID")
    nidinfo_parser.set_defaults(func=main_nidinfo)


    rtgens_parser = subparsers.add_parser("rtgens",parents=[parent_parser], help="Generate FGR routing map (serial)")
    rtgens_parser.set_defaults(func=main_rtgens)


    rtgenp_parser = subparsers.add_parser("rtgenp", parents=[parent_parser], help="Generate FGR routing map (parallel)")
    rtgenp_parser.set_defaults(func=main_rtgenp)


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


def do_fgrfile():
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
                lnet, rtr = [int(i) for i in ele[4:].split(":")]
                # G.ROUTERS.add(rtr)
                # if rtr not in G.LNET_ROUTERS[lnet]:
                #    G.LNET_ROUTERS[lnet].append(rtr)

                cost = 4 * dist(G.NID2X[nid], G.NID2X[rtr], 25)
                cost += 8 * dist(G.NID2Y[nid], G.NID2Y[rtr], 16)
                cost += dist(G.NID2Z[nid], G.NID2Z[rtr], 24)
                cost += 100  # TODO: for verification only

                G.ROUTER_COSTS[rtr][cost].append(nid)
                G.CLIENT_COSTS[nid][rtr] = cost


def do_mapfile():
    with open(ARGS.map, "r") as f:
        for line in f:
            nid, cname, nodetype, x, y, z = line.split()
            nid, x, y, z = map(int, [nid, x, y, z])

            if nodetype == "compute":
                G.CLIENTS.append(nid)
            G.NID2CNAME[nid] = cname
            G.NID2X[nid], G.NID2Y[nid], G.NID2Z[nid] = x, y, z
            G.CNAME2NID[cname] = nid

            create_rtr_list(cname, nid, x, y, z)


def do_nodefile():
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


def fgr_prepare(skip_fgr_file=False):
    """
    pre-processing
    """

    do_mapfile()

    do_nodefile()


    if ARGS.failed:
        G.CLIENTS = list(set(G.CLIENTS) - set(ARGS.failed))

    logger.info("G.CLIENTS contains [%s] nids", len(G.CLIENTS))

    if not skip_fgr_file:
        do_fgrfile()


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

    logger.info("Generating client 2 router cost:")
    with open("client2rtr.cost", "w") as f:
        pickle.dump(G.CLIENT_COSTS, f, pickle.HIGHEST_PROTOCOL)

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
        if client in G.SELECTED_CLIENT_IDS:
            continue
        else:
            break

    G.SELECTED_CLIENT_IDS.append(client)
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

    # check for duplicate
    import collections
    logger.info("Check duplicates: %s",
              [x for x, y in collections.Counter(G.SELECTED_CLIENT_IDS).items() if y > 1])

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
        fh.write("lfs setstripe -s %s -c 1 -i %s %s/%s\n"  % (ARGS.stripesize, ost, opath_mkdir, fname))

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

        f.write("aprun -n %s -N 1 -L %s %s -a POSIX -b 32g -e -E -F -i 1 -k -t 1m -vv -w -D 20 -o %s\n"
                % (ARGS.numranks, ",".join(clients), ARGS.iorbin, opath_ior))
        f.close()
    # set file permission
    os.chmod(ofile, 0744)

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



def gen_ofile_name():

    return "%s_%s_%s_%s.sh" % (ARGS.partition, ARGS.strategy, ARGS.numranks, ARGS.stripesize)



def rule1(cy, ry):
    """
    execute the Y-axis first selection algorithm
    """

    delta_y = (cy - ry + 24) % 16 - 8
    logger.debug("rtr.Y = %s, my.Y = %s, delta_Y = %s", ry, cy, delta_y)

    if -1 <= delta_y <= 2:
        return True
    else:
        return False



def select_grp(cname, rtrList):
    """
    Given a compute node (cname), we need to make a selection of router (modules)
    for the given router group.

    Each router group is divided into 4 sub-groups, each sub-group contains 3
    router modules.

    The selection strategy is Y-axis first, which will determine which sub-group
    gets chosen.

    Once a sub-group is chosen, then it consider X-axis, the distance along X
    will decide which router module in the sub-group is the primary router module

    cy is the Y coordinate for the compute node at question.

    ry is the Y coordinate for the router module. Each router module itself doesn't
       have any coordinate, we pick the first router node on that module, which
       is n0. As you can see from code:

           rnode = str2node(rtr3[0] + "n0"

    The Y-axis selection algorithm is implemented in rule1() function.
    The basic idea is to check if the compute node Y (cy) is one of the 4 values:

        ry-1, ry, ry+1, ry+2

    if it is, then this is the sub-group we choose. One router in this sub-group
    is the primary (based on X-axis), the other two are backups.

    if it is not, then we move to the next sub-group.

    There must be ONE sub-groups (among 4 of them) that meet the condition. Otherwise,
    it says there is not a router for this compute node to use to reach the LNET,
    which is impossible by design. If it happens, something is wrong.

    Y-dimension is 16. The router modules are placed such that, it covers all
    the points along Y-axis - 4 sub-groups, each sub-groups covers 4 points in space
    it should completely covers the Y-axis.

    """


    for i in range(4):
        idx = i * 3
        rtr3 = rtrList[idx:idx + 3]
        nid = G.CNAME2NID[cname]
        cy = G.NID2Y[nid]

        rnode = str2node(rtr3[0] + "n0")
        ry = rnode.y

        # TODO: if cy == y-1, y, y+1, y+2
        if rule1(cy, ry):
            return i, rtr3
    return None


def select_route(cname, lnet, rtrgrp):
    """

    node - a cname

    Once a sub-group (3 router modules)'s primary router module
    are picked for the this LNET, 3 other <LNET, ROUTER> mapping are also decided.

    This is due to the physical wiring that the router module has 4 router
    nodes. If the first router node (n0) is good for lnet X, then:

    n0 is good for LNET x
    n1 is good for LNET x+18
    n2 is good for LNET x+9
    n3 is good for LNET x+27

    So basically, each router node is connecting to one switch in that row.
    Remember, we have 4 rows of switches.

    GNI calculation:
       base GNI + (1, 4, 7, 10) + (0, 1, 2)

       1, 4, 7, 10 is the starting index of selected sub-group
       0, 1, 2 depending on which router is picked as primary: so if the first
       router picked as primary, it will use 0, second router picked as primary,
       it will use 1, third uses 2.


    TODO: this is reversed-engineered from dave's script, it matches the output
    That said, it doesn't make much sense yet: as the GNI lnet is not evenly spread
    Need to double check with the actual configuration.
    """

    # gindex tells which subgroup is picked.

    gindex, rtrpick = select_grp(cname, rtrgrp)

    if rtrpick is None:
        print "Can't locate router for node %s" % cname
        sys.exit(1)

    rtr3 = sort_rtr3(rtrpick)


    # c1 is the primary router module
    c1 = rtr3[0][0:-2]  # get rid of "n0"


    # rindex is index of the router that selected for primary
    # it should be one of 0, 1, 2
    rindex = rtrpick.index(c1)

    gni = G.BASE_GNI + (gindex * 3 + 1) + rindex


    G.LNET2RTR[lnet] = c1 + "n0"
    G.LNET2NID[lnet] = nid(c1 + 'n0')
    G.LNET2GNI[lnet] = gni

    G.LNET2RTR[lnet + 9] = c1 + "n2"
    G.LNET2NID[lnet + 9] = nid(c1 + 'n2')
    G.LNET2GNI[lnet + 9] = gni


    G.LNET2RTR[lnet + 18] = c1 + "n1"
    G.LNET2NID[lnet + 18] = nid(c1 + 'n1')
    G.LNET2GNI[lnet + 18] = gni

    G.LNET2RTR[lnet + 27] = c1 + "n3"
    G.LNET2NID[lnet + 27] = nid(c1 + 'n3')
    G.LNET2GNI[lnet + 27] = gni



def gen_routes(cname):
    """
    Given a node (cname), generate a list of 36 o2ib LNET and its
    corresponding *primary* router for that LNET

    There is also suppose to be a GNI number associated with that router
    And that GNI number is the identifier: when OSS sends reply back (return
    traffic), it tells which router to go to.

    Remember, this is only 12 router an OSS can choose to go back to the
    network, each router has a unique GNI LNET, so that GNI number uniquely
    tells which router to travel back to the (torus) network.

    As an example, NID 20 has cname of c0-0c1s5n0

    ./fgr.py node2route --map titan-system-map.20140131 --cname c0-0c1s5n0

        o2ib201:17736
        o2ib202:16906
        o2ib203:16930
        o2ib204:1540
        o2ib205:1564
        o2ib206:676
        o2ib207:700
        o2ib208:16204
        o2ib209:16180
        o2ib210:17686
        o2ib211:16980
        o2ib212:16956
        o2ib213:1626
        o2ib214:1602
        o2ib215:762
        o2ib216:738
        o2ib217:16146
        o2ib218:16170
        o2ib219:17737
        o2ib220:16907
        o2ib221:16931
        o2ib222:1541
        o2ib223:1565
        o2ib224:677
        o2ib225:701
        o2ib226:16205
        o2ib227:16181
        o2ib228:17687
        o2ib229:16981
        o2ib230:16957
        o2ib231:1627
        o2ib232:1603
        o2ib233:763
        o2ib234:739
        o2ib235:16147
        o2ib236:16171

    If we keep the original selection logic, the output is the same as:

    ./node-list-routes.sh c0-0c1s5n0

    201, rtr group A
    202, rtr group B
    ...
    210, rtr group I

    we call select_route() on each lnet ranging from 201 to 209.
    Each select_route will actually fill 4 mapping of <lnet, router ID>

    So after this, we will fill into all 4 * 9 = 36 LNET mapping with their
    primary router selection.

    """
    for i in range(9):
        lnet = G.BASE_O2IB + i
        select_route(cname, lnet, rtrALL[i])



def placement_random():
    random.seed()   # system time as seeds
    clients = map(str, random.sample(G.CLIENTS, ARGS.numranks))
    gen_shell(gen_ofile_name(), clients)

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
    gen_shell(gen_ofile_name())

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


def dump_routes():

    for key in G.LNET2NID.keys():
        print "o2ib%s:%s:gni%s" % (key, G.LNET2NID[key], G.LNET2GNI[key])




def record_routes(nid, f):
    f.write("%s " % nid)
    lnets = ["o2ib%s:%s" % (key, G.LNET2NID[key]) for key in G.LNET2NID.keys() ]
    f.write(" ".join(lnets))
    f.write("\n")


def main_rtgens():
    """
    serialized version
    TODO: still don't think G.CNAME is needed
    """
    fgr_prepare(skip_fgr_file=True)
    logger.info("Generating FGRFILE")
    f = open(ARGS.fgrfile, "w")
    for col in range(25):
        logger.info("\tprocessing %s of 25 columns", col+1)
        for row in range(8):
            for cage in range(3):
                for slot in range(8):
                    for n in range(4):
                        G.CNAME = "c%s-%sc%ss%sn%s" % (col, row, cage, slot, n)
                        gen_routes(G.CNAME)
                        record_routes(nid(G.CNAME), f)
    f.close()


def gen_routes_worker(row):
    name = multiprocessing.current_process().name
    logger.info("%s generating FGRFILE for row %s", name, row)
    fgr_partial = "%s.%02d" % (ARGS.fgrfile, row)
    f = open(fgr_partial, "w")
    for col in range(25):
        for cage in range(3):
            for slot in range(8):
                for n in range(4):
                    G.CNAME = "c%s-%sc%ss%sn%s" % (col, row, cage, slot, n)
                    G.CNODE = str2node(G.CNAME)
                    gen_routes(G.CNODE)
                    record_routes(f)
        percent = (col + 1) * 4
        logger.info("Worker %s: %d%% done", name, percent)
    f.close()
    logger.info("Finish generating routes for row %s", row)


def main_rtgenp():
    """using multiprocessing"""
    fgr_prepare(skip_fgr_file=True)
    jobs = []
    for i in range(8):
        p = multiprocessing.Process(target=gen_routes_worker, args=(i,))
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()

    logger.info("All jobs are finished, cats all output into a single one")

def main_nidinfo():
    """
    Given a NID, explore options
    """
    fgr_prepare()
    nid = ARGS.nid
    G.CNAME = G.NID2CNAME[nid]

    if not nid in G.CLIENTS:
        print("%s is not a compute node!" % nid)
        sys.exit(1)

    print("\nNID = %s, cname = %s, (%s, %s, %s)" %
          (nid, G.NID2CNAME[nid], G.NID2X[nid], G.NID2Y[nid], G.NID2Z[nid]))


    print("\nRouting Table for: %s\n" % nid)
    gen_routes(G.NID2CNAME[nid])
    dump_routes()


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
