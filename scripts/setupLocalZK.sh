#!/usr/bin/env bash

cfg=$(<cassandra-cfg.json)
nullValues=$(<null-values.json)

zookeepercli --servers localhost:21810 -c delete /streamsx.cassandra/cassandra_config
zookeepercli --servers localhost:21810 -c delete /streamsx.cassandra/null_values

zookeepercli --servers localhost:21810 -c creater /streamsx.cassandra/cassandra_config "$cfg"
zookeepercli --servers localhost:21810 -c creater /streamsx.cassandra/null_values "$nullValues"