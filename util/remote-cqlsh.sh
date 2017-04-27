#!/bin/bash

# usage: ./remote-cqlsh.sh <HOSTNAME>
# set CQLSH_PORT if non-9042

HOST=$1
docker run --rm -it cassandra cqlsh $HOST
