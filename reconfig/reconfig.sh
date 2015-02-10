#!/bin/bash

# The following arguments are passed to the script:
#
# <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
#
# <state> is currently always "failover"
# <role> is either "leader" or "observer"
#
# The arguments from-ip, from-port, to-ip, to-port are used to communicate
# the old address of the master and the new address of the elected slave
# (now a master).
master_name=$1
role=$2

reconfig=~/Work/github/go/src/github.com/Terry-Mao/goredis-sentinel-sdk/reconfig/reconfig
config=~/Work/github/go/src/github.com/Terry-Mao/goredis-sentinel-sdk/reconfig/${master_name}.conf
# if observer ignore
if test "${role}" == "observer"
then
    exit 0
fi
# if leader, do a reconfig to zk
${reconfig} -c ${config}
exit $?
