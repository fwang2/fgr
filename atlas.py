#!/usr/bin/env python
"""
    cross-validate lnet to ost mapping using Matt's atlas routine
"""

import string
import sys
from collections import defaultdict

class G:

    OSS_LIST = []
    OST_LIST = []

    LNET2OST1 = defaultdict(list)
    LNET2OST2 = defaultdict(list)

class OSS:

    def __init__(self, idx, name, fs, row, ddn, oss, switch, ip, o2ib, localosts):
        self.idx = idx
        self.name = name
        self.fs = fs
        self.row = row
        self.ddn = ddn
        self.oss = oss
        self.switch = switch
        self.ip = ip
        self.o2ib = o2ib
        self.localosts = localosts

class OST:
    def __init__(self, ost, name, fs, row, ddn, oss, switch, ip, o2ib, ossostidx):
        self.ost = ost
        self.name = name
        self.fs = fs
        self.row = row
        self.ddn = ddn
        self.oss = oss
        self.switch = switch
        self.ip = ip
        self.o2ib = o2ib
        self.ossostidx = ossostidx


# Calculate the atlas info
def create_atlas():
    letters = string.lowercase[:9]
    system  = "atlas"
    numrows = 4
    numssu  = 9
    numoss  = 8
    octet = [10, 36, 225, 30]

    for row in range(1, numrows + 1):
        fs = 1 if row <= 2 else 2
        for ssu in range(1, numssu + 1):
            ddn = letters[ssu - 1]
            for oss in range(1, numoss + 1):
                idx = oss + numoss * (ssu - 1) + numoss * numssu * (row - 1) - 1
                name = '%s-oss%d%s%d' % (system, row, ddn, oss)
                if oss <= 4:
                        switch = letters[ssu - 1]
                        o2ib = (row - 1) * numssu + ssu + 200
                else:
                        switch = letters[(ssu + numssu) % numssu]
                        o2ib = (row - 1) * numssu + (ssu + numssu) % numssu + 1 + 200
                if octet[3] > 254:
                        octet[3] = 1
                        octet[2] += 1
                ip = '.'.join(map(str, octet))
                oststart = idx if fs == 1 else idx - 144
                localosts = range(oststart, 1008, 144)
                G.OSS_LIST.append(OSS(idx, name, fs, row, ddn, oss, switch, ip, o2ib, localosts))
                for ossostidx, ost in enumerate(localosts):
                        G.OST_LIST.append(OST(ost, name, fs, row, ddn, oss, switch, ip, o2ib, ossostidx))
                octet[3] += 1

def read_from_disk():
    with open("lnet2ost.map", "r") as f:
        for line in f:
            if line.strip():
                line = line.split()
                lnet = int(line[0])
                # on-disk ost index for atlas2 starts from 1008
                G.LNET2OST2[lnet] = [x % 1008 for x in map(int, line[1:])]

def main():
    create_atlas()
    try:
        read_from_disk()
    except IOError, e:
        print(e)
        print("Please run \"./fgr2.py mapinfo\" first")
        sys.exit(1)

    for oss in G.OSS_LIST:
        o2ib, localosts = oss.o2ib, oss.localosts
        G.LNET2OST1[oss.o2ib].extend(oss.localosts)

    for lnet in G.LNET2OST1.keys():
        if set(G.LNET2OST1[lnet]) == set(G.LNET2OST2[lnet]):
            print("Checking %s: Okay" % lnet)
        else:
            print("Mismatch found: %s" % lnet)
            print("\t %s\n" % G.LNET2OST1[lnet])
            print("\t %s\n" % G.LNET2OST2[lnet])
            sys.exit(1)

if __name__ == "__main__": main()


