#!/usr/bin/env bash

cfg=$(<cfg)

zookeepercli --servers localhost:21810 -c creater /streamsx.sqs/pws_sqs_info "$cfg"