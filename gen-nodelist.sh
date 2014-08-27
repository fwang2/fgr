#!/bin/bash

xtprocadmin | awk '$4 == "compute" && $5 == "up" { print $1 }'

