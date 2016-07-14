#!/usr/bin/env bash

cfg=$(<cfg)

zookeepercli --servers localhost:21810 -c creater /streamsx.cassandra/hello_world_info "$cfg"