#!/usr/bin/env bash

URL=$1
while :
do
    if redis-cli -u $URL PING | grep -q PONG; then
        break
    fi
    sleep 1
done
