#!/usr/bin/env bash

cfg=$(<cfg.json)

zookeepercli --servers localhost:21810 -c delete /streamsx.cassandra/hello_world_info

zookeepercli --servers localhost:21810 -c creater /streamsx.cassandra/hello_world_info "$cfg"
